import argparse

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.photos import enrich_player_photos


def run_pipeline(with_photos: bool = False, photo_limit: int = 200):
    print("PIPELINE START")
    df = extract_data()
    df = transform_data(df)
    load_data(df)

    if with_photos:
        enrich_player_photos(limit=photo_limit)

    print("PIPELINE END")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-photos", action="store_true", help="Enriquecer con fotos de jugadores")
    parser.add_argument("--photo-limit", type=int, default=200, help="Cantidad de jugadores a consultar")
    args = parser.parse_args()

    run_pipeline(with_photos=args.with_photos, photo_limit=args.photo_limit)