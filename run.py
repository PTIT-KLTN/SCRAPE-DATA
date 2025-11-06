# from crawler.winmart.demo import main
# from crawler.bhx.demo import run_sync
from crawler.winmart.demo import run_sync

if __name__ == "__main__":
    run_sync(concurrency=2)