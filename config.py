import configargparse


CONFIG = None

arg_parser = configargparse.ArgParser(default_config_files=["config.txt"])

arg_parser.add("-c", "--config", is_config_file=True, help="Config file")

arg_parser.add(
    "-d",
    "--database_connection",
    help="A MongoDB connection string",
)

arg_parser.add(
    "-r",
    "--reload",
    default=False,
    help="Reload server during development",
)

arg_parser.add("--host", default="127.0.0.1", help="Bind socket to this host")

arg_parser.add("--port", default="8000", help="Bind socket to this port")

arg_parser.add("--file", default="Online_Retail.xlsx", help="CSV file containing data")

arg_parser.add("--db", default="test", help="mongo database")

arg_parser.add("--coll", default="invoices", help="collection to use")

arg_parser.add("--collection_input", help="collection from which to read")

arg_parser.add("--collection_output" help="collection in which to write")


def parse_args():
    global CONFIG
    CONFIG = arg_parser.parse_args()