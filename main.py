from lib.cli_args import CliArgs
from module.mainController import MainController


def main() -> None:
    cli = CliArgs()
    parsed_args = cli.parse()

    runner = MainController()
    runner.run(parsed_args, cli)


if __name__ == "__main__":
    main()
