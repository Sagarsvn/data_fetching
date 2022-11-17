from pathlib import Path
import pandas as pd

current_path = Path(__file__).parent.parent


def load_csv(path: str = "data/pandas_version.csv"):
    """Load csv file from path, to use this function please download data from this message slack
    then extract the data
    https://mnc-groupworkspace.slack.com/archives/C03RJG75HM5/p1668643931834189
    :param path: string path to csv file
    :return:
    """
    path = "{}/{}".format(current_path, path)
    df = pd.read_csv(path)
    print(len(df))


if __name__ == "__main__":
    load_csv()
