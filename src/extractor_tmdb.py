import requests
import pandas as pd
import os
from dotenv import load_dotenv
from tqdm import tqdm

# Carregar API key
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"


def get_params(**extra):
    return {
        "api_key": API_KEY,
        "language": "en-US",
        **extra
    }


def get_popular(media_type="movie", pages=1):
    print(f"Coletando {media_type}s populares...")
    all_items = []
    for page in range(1, pages + 1):
        url = f"{BASE_URL}/{media_type}/popular"
        response = requests.get(url, params=get_params(page=page))
        data = response.json().get("results", [])
        for item in data:
            all_items.append({
                "id": item.get("id"),
                "title": item.get("title") or item.get("name"),
                "popularity": item.get("popularity"),
                "vote_average": item.get("vote_average"),
                "vote_count": item.get("vote_count"),
                "media_type": media_type
            })
    return pd.DataFrame(all_items)


def get_reviews(media_id, media_type="movie"):
    url = f"{BASE_URL}/{media_type}/{media_id}/reviews"
    response = requests.get(url, params=get_params(page=1))
    if response.status_code == 200:
        return response.json().get("results", [])
    return []


def get_details(media_id, media_type="movie"):
    url = f"{BASE_URL}/{media_type}/{media_id}"
    response = requests.get(url, params=get_params())
    return response.json() if response.status_code == 200 else {}


def get_genres(media_type="movie"):
    url = f"{BASE_URL}/genre/{media_type}/list"
    response = requests.get(url, params=get_params())
    return response.json().get("genres", [])


# Pipeline principal
def run_pipeline(media_type="movie", pages=1):
    df_items = get_popular(media_type, pages)
    print(f"🔎 {len(df_items)} {media_type}s coletados.")

    genres_map = {g['id']: g['name'] for g in get_genres(media_type)}

    reviews_data = []
    details_data = []

    for _, row in tqdm(df_items.iterrows(), total=df_items.shape[0]):
        media_id = row["id"]
        title = row["title"]

        # Reviews
        reviews = get_reviews(media_id, media_type)
        for r in reviews:
            reviews_data.append({
                "media_id": media_id,
                "title": title,
                "author": r.get("author"),
                "content": r.get("content"),
                "created_at": r.get("created_at")
            })

        # Detalhes
        details = get_details(media_id, media_type)
        genres_names = [genres_map.get(g['id']) for g in details.get("genres", [])]

        details_data.append({
            "media_id": media_id,
            "title": title,
            "overview": details.get("overview"),
            "release_date": details.get("release_date") or details.get("first_air_date"),
            "genres": ", ".join(filter(None, genres_names)),
            "original_language": details.get("original_language"),
            "runtime": details.get("runtime") or details.get("episode_run_time", [None])[0]
        })

    # Salvar em caminho absoluto fixo
    folder = r"data\raw"
    os.makedirs(folder, exist_ok=True)

    df_items.to_csv(os.path.join(folder, f"raw_{media_type}s.csv"), index=False)
    pd.DataFrame(reviews_data).to_csv(os.path.join(folder, f"reviews_{media_type}s.csv"), index=False)
    pd.DataFrame(details_data).to_csv(os.path.join(folder, f"details_{media_type}s.csv"), index=False)

    print(f"✅ Dados salvos em {folder} para {media_type}s.")


if __name__ == "__main__":
    run_pipeline("movie", pages=4)  # Ajuste o número de páginas aqui
    run_pipeline("tv", pages=4)
