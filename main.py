import argparse
import sys
# TODO: Teammate 1 - Import the consumer and producer modules here once they are written

def main():
    parser = argparse.ArgumentParser(description="GitHub Streaming Analytics Pipeline")
    parser.add_argument("--service", required=True, help="Service to run (producer, consumer-language-stats, etc.)")
    args = parser.parse_args()

    # TODO: Teammate 2 - Implement the if/elif routing logic here to start the correct service
    print(f"Starting service: {args.service} (Logic pending implementation)")

if __name__ == "__main__":
    main()
