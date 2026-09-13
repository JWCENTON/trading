"""One bounded, process-owned schedule for the existing LOCAL PAPER consumer.

No new service, watermark or replay. One batch, then a full 60s cooldown;
slow/failed batches never trigger catch-up bursts.
"""
import logging
import threading

POLL_COOLDOWN_SECONDS = 60
POLL_BUDGET_SECONDS = 15


def local_paper_schedule(env, trading_mode):
    return (trading_mode == "PAPER" and env.get("DEPLOYMENT_ID") == "local-paper"
            and env.get("DB_NAME") == "trading_paper")


class OutboxSchedule:
    def __init__(self, poll):
        self.poll = poll
        self.stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None

    def start(self):
        with self._lock:
            if self._thread is not None:
                return  # never install a second schedule in this process
            self._thread = threading.Thread(target=self.run, name="causal-outbox", daemon=True)
            self._thread.start()

    def run(self):
        while not self.stop.is_set():
            try:
                count = self.poll(max_duration_seconds=POLL_BUDGET_SECONDS)
                logging.info("causal_observation_consumer schedule=independent processed=%s", count)
            except Exception as exc:
                # No SQL payload/credentials in diagnostics.
                logging.error("causal_observation_consumer schedule=independent error_class=%s", type(exc).__name__)
            if self.stop.wait(POLL_COOLDOWN_SECONDS):
                return
