"""
Spams the API to test load capability and stability.
"""

import asyncio
import curses
import time
import httpx
from collections import deque

# Configuration
URL = "http://localhost:8001/track/?id=194567102&quality=HI_RES_LOSSLESS"  # Default URL

async def make_request(client, url, stats):
    try:
        start = time.time()
        resp = await client.get(url)
        latency = time.time() - start
        stats['total'] += 1
        stats['latency'].append(latency)
        if len(stats['latency']) > 100:
            stats['latency'].popleft()
        
        if resp.status_code == 200:
            stats['success'] += 1
        else:
            stats['fail'] += 1
            stats['last_error'] = f"Status {resp.status_code}"
    except Exception as e:
        stats['total'] += 1
        stats['fail'] += 1
        stats['last_error'] = str(e)

async def worker(stdscr):
    # Setup curses
    curses.curs_set(0)
    stdscr.nodelay(True)
    
    # State
    rate = 100  # Requests per second
    stats = {
        'total': 0,
        'success': 0,
        'fail': 0,
        'latency': deque(),
        'last_error': "None"
    }
    
    # Async client
    async with httpx.AsyncClient() as client:
        last_check = time.time()
        tokens = 0.0
        
        while True:
            # Handle Input
            try:
                key = stdscr.getch()
                if key == curses.KEY_UP:
                    rate += 1
                elif key == curses.KEY_DOWN:
                    rate = max(1, rate - 1)
                elif key == curses.KEY_RIGHT:
                    rate += 10
                elif key == curses.KEY_LEFT:
                    rate = max(1, rate - 10)
                elif key == ord('q'):
                    break
            except:
                pass

            # Update tokens based on time passed
            now = time.time()
            dt = now - last_check
            last_check = now
            
            tokens += dt * rate
            
            # Cap tokens to avoid massive bursts if we stall
            if tokens > rate: 
                tokens = rate

            # Spend tokens
            while tokens >= 1.0:
                asyncio.create_task(make_request(client, URL, stats))
                tokens -= 1.0

            # Draw UI
            try:
                stdscr.erase()
                stdscr.addstr(0, 0, f"API Spammer - Target: {URL}")
                stdscr.addstr(2, 0, f"Target Rate: {rate} req/s (Arrows to adjust)")
                stdscr.addstr(4, 0, f"Total Requests: {stats['total']}")
                stdscr.addstr(5, 0, f"Success: {stats['success']}")
                stdscr.addstr(6, 0, f"Failed: {stats['fail']}")
                
                avg_lat = 0
                if stats['latency']:
                    avg_lat = sum(stats['latency']) / len(stats['latency']) * 1000
                stdscr.addstr(7, 0, f"Avg Latency (last 100): {avg_lat:.2f} ms")
                stdscr.addstr(8, 0, f"Last Error: {stats['last_error']}")
                stdscr.addstr(10, 0, "Press 'q' to quit")
                stdscr.refresh()
            except curses.error:
                pass # Ignore resize errors etc

            await asyncio.sleep(0.01)

def main(stdscr):
    try:
        asyncio.run(worker(stdscr))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    curses.wrapper(main)
