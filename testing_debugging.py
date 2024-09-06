import os
import requests

#print("TMDB API Key:", os.getenv('TMDB_API_KEY'))
#print("Database Password:", os.getenv('DB_PASSWORD'))

TMDB_API_KEY = os.getenv('TMDB_API_KEY')

def get_total_pages():
    url = 'https://api.themoviedb.org/3/discover/movie'
    params = {
        'api_key': TMDB_API_KEY,
        'sort_by': 'popularity.desc',
        'page': 1  # Start with the first page
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    total_pages = data['total_pages']
    return total_pages

# Example usage
total_pages = get_total_pages()
print(f"Total number of pages: {total_pages}")