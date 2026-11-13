
import argparse

from src.results.results_application import ResultsApplication


class ResultsProcessorCLI:
    """
    Command-line interface for the Results Processing Service.
    """

    def __init__(self):

        self._parser = argparse.ArgumentParser(
            description="Kafka AI Monitoring Results Processor"
        )

        self._parser.add_argument(
            "--once",
            action="store_true",
            help="Process one result and exit",
        )

    def run(self) -> None:

        args = self._parser.parse_args()

        application = None

        try:

            application = ResultsApplication()

            application.run(
                max_messages=1 if args.once else None
            )

        except KeyboardInterrupt:

            print(
                "\nResults Processing Service "
                "stopped by user."
            )

        finally:

            print("Results Processor CLI terminated.")


if __name__ == "__main__":
    ResultsProcessorCLI().run()
