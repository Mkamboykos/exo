import argparse
import multiprocessing as mp
import os
import resource
import signal
import sys
from dataclasses import dataclass, field
from typing import Final, Self

import anyio
from loguru import logger
from pydantic import PositiveInt

import exo.routing.topics as topics
from exo.api.main import API
from exo.download.coordinator import DownloadCoordinator
from exo.download.impl_shard_downloader import exo_shard_downloader
from exo.master.main import Master
from exo.routing.event_router import EventRouter
from exo.routing.router import Router, get_node_id_keypair
from exo.shared.constants import EXO_DEFAULT_MODELS_DIR, EXO_LOG
from exo.shared.election import Election, ElectionResult
from exo.shared.logging import logger_cleanup, logger_setup
from exo.shared.types.common import NodeId, SessionId
from exo.utils.channels import Receiver, channel
from exo.utils.daemon import detach_stdio_to_devnull
from exo.utils.pidfile import PidfileLockError, acquire_exo_pidfile
from exo.utils.pydantic_ext import FrozenModel
from exo.utils.task_group import TaskGroup
from exo.worker.main import Worker


@dataclass
class Node:
    router: Router
    event_router: EventRouter
    download_coordinator: DownloadCoordinator | None
    worker: Worker | None
    election: Election  # Every node participates in election, as we do want a node to become master even if it isn't a master candidate if no master candidates are present.
    election_result_receiver: Receiver[ElectionResult]
    master: Master | None
    api: API | None

    node_id: NodeId
    offline: bool
    _api_port: int
    _tg: TaskGroup = field(init=False, default_factory=TaskGroup)

    @classmethod
    async def create(cls, args: "Args") -> Self:
        keypair = get_node_id_keypair()
        node_id = NodeId(keypair.to_node_id())
        session_id = SessionId(master_node_id=node_id, election_clock=0)
        router = Router.create(
            keypair,
            bootstrap_peers=args.bootstrap_peers,
            listen_port=args.libp2p_port,
            listen_address=args.listen_address,
        )
        await router.register_topic(topics.GLOBAL_EVENTS)
        await router.register_topic(topics.LOCAL_EVENTS)
        await router.register_topic(topics.COMMANDS)
        await router.register_topic(topics.ELECTION_MESSAGES)
        await router.register_topic(topics.CONNECTION_MESSAGES)
        await router.register_topic(topics.DOWNLOAD_COMMANDS)
        event_router = EventRouter(
            session_id,
            command_sender=router.sender(topics.COMMANDS),
            external_outbound=router.sender(topics.LOCAL_EVENTS),
            external_inbound=router.receiver(topics.GLOBAL_EVENTS),
        )

        logger.info(f"Starting node {node_id}")

        # Errors the very first time exo is run as dir doesn't exist
        EXO_DEFAULT_MODELS_DIR.mkdir(parents=True, exist_ok=True)

        # Create DownloadCoordinator (unless --no-downloads)
        if not args.no_downloads:
            download_coordinator = DownloadCoordinator(
                node_id,
                exo_shard_downloader(offline=args.offline),
                event_sender=event_router.sender(),
                download_command_receiver=router.receiver(topics.DOWNLOAD_COMMANDS),
                offline=args.offline,
            )
        else:
            download_coordinator = None

        if args.spawn_api:
            api = API(
                node_id,
                port=args.api_port,
                event_receiver=event_router.receiver(),
                command_sender=router.sender(topics.COMMANDS),
                download_command_sender=router.sender(topics.DOWNLOAD_COMMANDS),
                election_receiver=router.receiver(topics.ELECTION_MESSAGES),
            )
        else:
            api = None

        if not args.no_worker:
            worker = Worker(
                node_id,
                event_receiver=event_router.receiver(),
                event_sender=event_router.sender(),
                command_sender=router.sender(topics.COMMANDS),
                download_command_sender=router.sender(topics.DOWNLOAD_COMMANDS),
                api_port=args.api_port,
            )
        else:
            worker = None

        # We start every node with a master
        master = Master(
            node_id,
            session_id,
            event_sender=event_router.sender(),
            global_event_sender=router.sender(topics.GLOBAL_EVENTS),
            local_event_receiver=router.receiver(topics.LOCAL_EVENTS),
            command_receiver=router.receiver(topics.COMMANDS),
            download_command_sender=router.sender(topics.DOWNLOAD_COMMANDS),
        )

        er_send, er_recv = channel[ElectionResult]()
        election = Election(
            node_id,
            # If someone manages to assemble 1 MILLION devices into an exo cluster then. well done. good job champ.
            seniority=1_000_000 if args.force_master else 0,
            # nb: this DOES feedback right now. i have thoughts on how to address this,
            # but ultimately it seems not worth the complexity
            election_message_sender=router.sender(topics.ELECTION_MESSAGES),
            election_message_receiver=router.receiver(topics.ELECTION_MESSAGES),
            connection_message_receiver=router.receiver(topics.CONNECTION_MESSAGES),
            command_receiver=router.receiver(topics.COMMANDS),
            election_result_sender=er_send,
        )

        return cls(
            router,
            event_router,
            download_coordinator,
            worker,
            election,
            er_recv,
            master,
            api,
            node_id,
            args.offline,
            args.api_port,
        )

    async def run(self):
        async with self._tg as tg:
            signal.signal(signal.SIGINT, lambda _, __: self.shutdown())
            signal.signal(signal.SIGTERM, lambda _, __: self.shutdown())
            tg.start_soon(self.router.run)
            tg.start_soon(self.event_router.run)
            tg.start_soon(self.election.run)
            if self.download_coordinator:
                tg.start_soon(self.download_coordinator.run)
            if self.worker:
                tg.start_soon(self.worker.run)
            if self.master:
                tg.start_soon(self.master.run)
            if self.api:
                tg.start_soon(self.api.run)
            tg.start_soon(self._elect_loop)

    def shutdown(self):
        # if this is our second call to shutdown, just sys.exit
        if self._tg.cancel_called():
            import sys

            sys.exit(1)
        self._tg.cancel_tasks()

    async def _elect_loop(self):
        with self.election_result_receiver as results:
            async for result in results:
                # This function continues to have a lot of very specific entangled logic
                # At least it's somewhat contained

                # I don't like this duplication, but it's manageable for now.
                # TODO: This function needs refactoring generally

                # Ok:
                # On new master:
                # - Elect master locally if necessary
                # - Shutdown and re-create the worker
                # - Shut down and re-create the API

                if result.is_new_master:
                    await anyio.sleep(0)
                    self.event_router.shutdown()
                    self.event_router = EventRouter(
                        result.session_id,
                        self.router.sender(topics.COMMANDS),
                        self.router.receiver(topics.GLOBAL_EVENTS),
                        self.router.sender(topics.LOCAL_EVENTS),
                    )

                if (
                    result.session_id.master_node_id == self.node_id
                    and self.master is not None
                ):
                    logger.info("Node elected Master")
                elif (
                    result.session_id.master_node_id == self.node_id
                    and self.master is None
                ):
                    logger.info("Node elected Master - promoting self")
                    self.master = Master(
                        self.node_id,
                        result.session_id,
                        event_sender=self.event_router.sender(),
                        global_event_sender=self.router.sender(topics.GLOBAL_EVENTS),
                        local_event_receiver=self.router.receiver(topics.LOCAL_EVENTS),
                        command_receiver=self.router.receiver(topics.COMMANDS),
                        download_command_sender=self.router.sender(
                            topics.DOWNLOAD_COMMANDS
                        ),
                    )
                    self._tg.start_soon(self.master.run)
                elif (
                    result.session_id.master_node_id != self.node_id
                    and self.master is not None
                ):
                    logger.info(
                        f"Node {result.session_id.master_node_id} elected master - demoting self"
                    )
                    await self.master.shutdown()
                    self.master = None
                else:
                    logger.info(
                        f"Node {result.session_id.master_node_id} elected master"
                    )
                if result.is_new_master:
                    if self.download_coordinator:
                        await self.download_coordinator.shutdown()
                        self.download_coordinator = DownloadCoordinator(
                            self.node_id,
                            exo_shard_downloader(offline=self.offline),
                            event_sender=self.event_router.sender(),
                            download_command_receiver=self.router.receiver(
                                topics.DOWNLOAD_COMMANDS
                            ),
                            offline=self.offline,
                        )
                        self._tg.start_soon(self.download_coordinator.run)
                    if self.worker:
                        await self.worker.shutdown()
                        # TODO: add profiling etc to resource monitor
                        self.worker = Worker(
                            self.node_id,
                            event_receiver=self.event_router.receiver(),
                            event_sender=self.event_router.sender(),
                            command_sender=self.router.sender(topics.COMMANDS),
                            download_command_sender=self.router.sender(
                                topics.DOWNLOAD_COMMANDS
                            ),
                            api_port=self._api_port,
                        )
                        self._tg.start_soon(self.worker.run)
                    if self.api:
                        self.api.reset(result.won_clock, self.event_router.receiver())
                    self._tg.start_soon(self.event_router.run)
                else:
                    if self.api:
                        self.api.unpause(result.won_clock)


# Ordered list of interfaces to check for a static Thunderbolt IP.
# setup_thunderbolt.sh assigns the IP to the active Thunderbolt port (en1/en2/en3)
# directly rather than to bridge0, which has an IP routing bug on macOS
# (sendto returns EHOSTUNREACH despite valid ARP). bridge0 is kept first for
# backwards compatibility with users who assigned the IP there manually.
_THUNDERBOLT_IFACE_CANDIDATES: Final[list[str]] = ["bridge0", "en1", "en2", "en3"]
_THUNDERBOLT_AUTO_PORT = 50001


def _is_link_local(ip: str) -> bool:
    """Return True if the address is an IPv4 link-local address (169.254.x.x)."""
    return ip.startswith("169.254.")


def _detect_thunderbolt_iface_and_ip() -> tuple[str, str] | None:
    """Return (iface, ip) for the first Thunderbolt interface that has a static IPv4 address.

    Checks bridge0 then en1/en2/en3. setup_thunderbolt.sh assigns the IP directly
    to the active Thunderbolt port (en1/en2/en3) because bridge0 has an IP routing
    bug on macOS where sendto() returns EHOSTUNREACH despite a valid ARP entry.
    bridge0 is still checked first for backwards compatibility.

    Only returns static (non-link-local) addresses.
    Returns None if no candidate interface has a static IPv4 address.
    """
    import socket

    import psutil

    stats = psutil.net_if_stats()
    addrs = psutil.net_if_addrs()

    for iface in _THUNDERBOLT_IFACE_CANDIDATES:
        iface_stats = stats.get(iface)
        iface_addrs = addrs.get(iface)
        if iface_stats is None or iface_addrs is None or not iface_stats.isup:
            continue
        for addr in iface_addrs:
            if addr.family == socket.AF_INET and not addr.address.startswith("127.") and not _is_link_local(addr.address):
                return iface, addr.address
    return None


def _detect_thunderbolt_bridge_ip() -> str | None:
    """Return the static IPv4 address of the active Thunderbolt interface, or None."""
    result = _detect_thunderbolt_iface_and_ip()
    return result[1] if result is not None else None


def _is_multicast(ip: str) -> bool:
    """Return True for any IPv4 multicast address (224.0.0.0/4, i.e. first octet 224-239)."""
    try:
        return 224 <= int(ip.split(".")[0]) <= 239
    except (ValueError, IndexError):
        return False


def _detect_thunderbolt_peer_ips(local_ip: str) -> list[str]:
    """Return IPv4 addresses of peers discovered via ARP on the active Thunderbolt interface.

    Filters out the local interface IP, multicast addresses (224-239.x.x.x),
    broadcast addresses (x.x.x.255), and link-local addresses (169.254.x.x).
    """
    import re
    import subprocess

    tb = _detect_thunderbolt_iface_and_ip()
    iface = tb[0] if tb is not None else "bridge0"

    try:
        result = subprocess.run(
            ["arp", "-a", "-i", iface],
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []

    # Each line: hostname (1.2.3.4) at aa:bb:cc:dd:ee:ff on bridge0 [ethernet]
    peers: list[str] = []
    for line in result.stdout.splitlines():
        match = re.search(r"\((\d+\.\d+\.\d+\.\d+)\)", line)
        if match:
            ip = match.group(1)
            if (
                ip != local_ip
                and not _is_multicast(ip)
                and not ip.endswith(".255")
                and not _is_link_local(ip)
            ):
                peers.append(ip)
    return peers


def main():
    # Exit early if no PID file (not compatible with double-for daemonization yet)
    try:
        pidfile = acquire_exo_pidfile()
    except PidfileLockError as exception:
        print(exception, file=sys.stderr)
        raise SystemExit(1) from exception

    args = Args.parse()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = min(max(soft, 65535), hard)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))

    mp.set_start_method("spawn", force=True)

    # TODO: Refactor the current verbosity system
    logger_setup(EXO_LOG, args.verbosity)
    if args.no_stdio:
        detach_stdio_to_devnull()
        logger.info("Detached stdio to /dev/null")

    logger.info(f"{'=' * 40}")
    logger.info(f"Starting EXO | pid={os.getpid()}")
    logger.info(f"{'=' * 40}")
    logger.info(f"EXO_LIBP2P_NAMESPACE: {os.getenv('EXO_LIBP2P_NAMESPACE')}")

    if args.listen_address:
        logger.info(
            f"Binding libp2p to {args.listen_address}:{args.libp2p_port} "
            f"(Thunderbolt Bridge auto-detected)"
        )
        if args.bootstrap_peers:
            logger.info(f"Thunderbolt peers auto-discovered via ARP: {args.bootstrap_peers}")

    if args.offline:
        logger.info("Running in OFFLINE mode — no internet checks, local models only")

    if args.bootstrap_peers:
        logger.info(f"Bootstrap peers: {args.bootstrap_peers}")

    if args.no_batch:
        os.environ["EXO_NO_BATCH"] = "1"
        logger.info("Continuous batching disabled (--no-batch)")

    # Set FAST_SYNCH override env var for runner subprocesses
    if args.fast_synch is True:
        os.environ["EXO_FAST_SYNCH"] = "true"
        logger.info("FAST_SYNCH forced ON")
    elif args.fast_synch is False:
        os.environ["EXO_FAST_SYNCH"] = "false"
        logger.info("FAST_SYNCH forced OFF")

    node = anyio.run(Node.create, args)
    try:
        anyio.run(node.run)
    except BaseException as exception:
        logger.opt(exception=exception).critical(
            "EXO terminated due to unhandled exception"
        )
        raise
    finally:
        logger.info("EXO Shutdown complete")
        logger_cleanup()
        del pidfile


class Args(FrozenModel):
    verbosity: int = 0
    force_master: bool = False
    spawn_api: bool = False
    api_port: PositiveInt = 52415
    tb_only: bool = False
    no_worker: bool = False
    no_downloads: bool = False
    offline: bool = os.getenv("EXO_OFFLINE", "false").lower() == "true"
    no_batch: bool = False
    fast_synch: bool | None = None  # None = auto, True = force on, False = force off
    no_stdio: bool = False
    bootstrap_peers: list[str] = []
    libp2p_port: int
    listen_address: str | None = None

    @classmethod
    def parse(cls) -> Self:
        parser = argparse.ArgumentParser(prog="EXO")
        default_verbosity = 0
        parser.add_argument(
            "-q",
            "--quiet",
            action="store_const",
            const=-1,
            dest="verbosity",
            default=default_verbosity,
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="count",
            dest="verbosity",
            default=default_verbosity,
        )
        parser.add_argument(
            "-m",
            "--force-master",
            action="store_true",
            dest="force_master",
        )
        parser.add_argument(
            "--no-api",
            action="store_false",
            dest="spawn_api",
        )
        parser.add_argument(
            "--api-port",
            type=int,
            dest="api_port",
            default=52415,
        )
        parser.add_argument(
            "--no-worker",
            action="store_true",
        )
        parser.add_argument(
            "--no-downloads",
            action="store_true",
            help="Disable the download coordinator (node won't download models)",
        )
        parser.add_argument(
            "--offline",
            action="store_true",
            default=os.getenv("EXO_OFFLINE", "false").lower() == "true",
            help="Run in offline/air-gapped mode: skip internet checks, use only pre-staged local models",
        )
        parser.add_argument(
            "--no-batch",
            action="store_true",
            help="Disable continuous batching, use sequential generation",
        )
        parser.add_argument(
            "--no-stdio",
            action="store_true",
            help="Detach stdin/stdout/stderr to /dev/null after logging is configured",
        )
        parser.add_argument(
            "--bootstrap-peers",
            type=lambda s: [p for p in s.split(",") if p],
            default=os.getenv("EXO_BOOTSTRAP_PEERS", "").split(",")
            if os.getenv("EXO_BOOTSTRAP_PEERS")
            else [],
            dest="bootstrap_peers",
            help="Comma-separated libp2p multiaddrs to dial on startup (env: EXO_BOOTSTRAP_PEERS)",
        )
        parser.add_argument(
            "--libp2p-port",
            type=int,
            default=0,
            dest="libp2p_port",
            help="Fixed TCP port for libp2p to listen on (0 = OS-assigned).",
        )
        parser.add_argument(
            "--listen-address",
            type=str,
            default=os.getenv("EXO_LISTEN_ADDRESS") or None,
            dest="listen_address",
            help="IPv4 address for libp2p to bind to (env: EXO_LISTEN_ADDRESS). Defaults to all interfaces. Set to your Thunderbolt Bridge IP to route traffic through a direct cable connection.",
        )
        parser.add_argument(
            "--tb-only",
            action="store_true",
            dest="tb_only",
            help="Require a Thunderbolt Bridge (bridge0) to be present and exit if one is not detected. "
                 "Useful when deploying to machines that should only communicate over a direct cable connection.",
        )
        fast_synch_group = parser.add_mutually_exclusive_group()
        fast_synch_group.add_argument(
            "--fast-synch",
            action="store_true",
            dest="fast_synch",
            default=None,
            help="Force MLX FAST_SYNCH on (for JACCL backend)",
        )
        fast_synch_group.add_argument(
            "--no-fast-synch",
            action="store_false",
            dest="fast_synch",
            help="Force MLX FAST_SYNCH off",
        )

        raw = vars(parser.parse_args())

        # Auto-detect Thunderbolt Bridge — bind to it when no explicit listen address is given.
        if not raw.get("listen_address"):
            tb_ip = _detect_thunderbolt_bridge_ip()
            if tb_ip is not None:
                raw["listen_address"] = tb_ip
                # Use a fixed port so peers can reliably bootstrap to us.
                if raw.get("libp2p_port", 0) == 0:
                    raw["libp2p_port"] = _THUNDERBOLT_AUTO_PORT
                # Auto-add ARP-discovered peers as bootstrap peers if none are configured.
                if not raw.get("bootstrap_peers"):
                    peer_ips = _detect_thunderbolt_peer_ips(tb_ip)
                    if peer_ips:
                        raw["bootstrap_peers"] = [
                            f"/ip4/{ip}/tcp/{_THUNDERBOLT_AUTO_PORT}" for ip in peer_ips
                        ]
            elif raw.get("tb_only"):
                parser.error(
                    f"--tb-only specified but no Thunderbolt interface ({', '.join(_THUNDERBOLT_IFACE_CANDIDATES)}) "
                    "was detected with a static IP. Ensure a Thunderbolt cable is connected and "
                    "that the interface has an IP address (run ./scripts/setup_thunderbolt.sh if needed)."
                )

        return cls(**raw)  # pyright: ignore[reportAny] - We are intentionally validating here, we can't do it statically
