import asyncio
import httpx
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:8000"

# Sample IDs for testing (these might need to be updated if they become invalid on Tidal)
SAMPLE_TRACK_ID = 194567102  # Example track ID
SAMPLE_ALBUM_ID = 56681092 # Example album ID
SAMPLE_ARTIST_ID = 9321197     # Example artist ID (Daft Punk)
SAMPLE_VIDEO_ID = 402643865  # Example video ID (might need a real one)
SAMPLE_PLAYLIST_ID = "1c5d01ed-4f05-40c4-bd28-0f730f9b220b" # Example playlist UUID
SAMPLE_MIX_ID = "00112233445566778899aabbcc" # Example mix ID

async def test_endpoint(client: httpx.AsyncClient, name: str, path: str, params: dict = None):
    url = f"{BASE_URL}{path}"
    start_time = time.time()
    try:
        response = await client.get(url, params=params, timeout=15.0)
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            logger.info(f"✅ [PASS] {name} ({path}) - {elapsed:.2f}s")
            return True
        else:
            logger.error(f"❌ [FAIL] {name} ({path}) - Status: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"❌ [ERROR] {name} ({path}) - {elapsed:.2f}s - {str(e)}")
        return False

async def run_all_tests():
    logger.info(f"Starting API tests against {BASE_URL}...")
    
    async with httpx.AsyncClient() as client:
        # 1. Root Endpoint
        await test_endpoint(client, "Root Index", "/")
        
        # 2. Track Info
        await test_endpoint(client, "Track Info", "/info/", {"id": SAMPLE_TRACK_ID})
        
        # 3. Track Playback Info
        await test_endpoint(client, "Track Playback", "/track/", {"id": SAMPLE_TRACK_ID})
        
        # 4. Track Recommendations
        await test_endpoint(client, "Track Recommendations", "/recommendations/", {"id": SAMPLE_TRACK_ID})
        
        # 5. Search (Tracks)
        await test_endpoint(client, "Search Tracks", "/search/", {"s": "daft punk"})
        
        # 6. Search (Artists)
        await test_endpoint(client, "Search Artists", "/search/", {"a": "daft punk"})
        
        # 7. Album Details
        await test_endpoint(client, "Album Details", "/album/", {"id": SAMPLE_ALBUM_ID})
        
        # 8. Album Similar
        await test_endpoint(client, "Similar Albums", "/album/similar/", {"id": SAMPLE_ALBUM_ID})
        
        # 9. Artist Details (Basic)
        await test_endpoint(client, "Artist Details", "/artist/", {"id": SAMPLE_ARTIST_ID})
        
        # 10. Artist Albums & Tracks
        await test_endpoint(client, "Artist Albums/Tracks", "/artist/", {"f": SAMPLE_ARTIST_ID})
        
        # 11. Artist Similar
        await test_endpoint(client, "Similar Artists", "/artist/similar/", {"id": SAMPLE_ARTIST_ID})
        
        # 12. Cover by Track ID
        await test_endpoint(client, "Cover by ID", "/cover/", {"id": SAMPLE_TRACK_ID})
        
        # 13. Cover by Query
        await test_endpoint(client, "Cover by Query", "/cover/", {"q": "discovery daft punk"})
        
        # 14. Lyrics
        await test_endpoint(client, "Lyrics", "/lyrics/", {"id": SAMPLE_TRACK_ID})
        
        # 15. Top Videos
        await test_endpoint(client, "Top Videos", "/topvideos/")
        
        # 16. Playlist
        await test_endpoint(client, "Playlist", "/playlist/", {"id": SAMPLE_PLAYLIST_ID})
        
        # Note: /mix/ and /video/ might fail if the sample IDs are invalid or expired, 
        # but we test them anyway to ensure the endpoint structure works.
        # await test_endpoint(client, "Mix", "/mix/", {"id": SAMPLE_MIX_ID})
        # await test_endpoint(client, "Video Playback", "/video/", {"id": SAMPLE_VIDEO_ID})

if __name__ == "__main__":
    asyncio.run(run_all_tests())
