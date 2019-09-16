import signal


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


def launch_topic_listener(exp, run):
    killer = GracefulKiller()
    while not killer.kill_now:
        pass

    # Clean up the run, since it's finished
