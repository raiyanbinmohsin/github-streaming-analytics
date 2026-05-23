import json
import logging
import os
import time
from collections import defaultdict
import pulsar
import requests

# Q3: Top 10 languages following test-driven development (most projects with unit tests)

TEST_FOLDERS = {"test", "tests", "spec", "specs", "__tests__", "testing"}
LOGGER = logging.getLogger("consumer.unit_test_analysis")

def has_test_folder(contents_url: str, token: str) -> bool:
    """
    Hits the GitHub Contents API to check the root directory for test folders.
    """
    if not contents_url:
        return False
        
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        response = requests.get(contents_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            contents = response.json()
            if isinstance(contents, list):
                for item in contents:
                    if item.get("type") == "dir" and item.get("name", "").lower() in TEST_FOLDERS:
                        return True
            return False
            
        elif response.status_code == 403:
            LOGGER.warning("Rate limit hit on contents API. Pausing...")
            time.sleep(60) 
            return False
            
    except Exception as e:
        LOGGER.warning("Error fetching contents: %s", e)
        
    return False

def save_results(tdd_language_counts):
    """Persist Q3 language counts to the shared results JSON file."""
    top_n = int(os.getenv("TOP_N", "10"))
    
    # Sort languages by how many TDD projects they have
    sorted_tdd_languages = sorted(tdd_language_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    results = {
        "Q3_top_tdd_languages": sorted_tdd_languages
    }
    
    os.makedirs("/results", exist_ok=True)
    with open("/results/unit_test_analysis.json", "w") as f:
        json.dump(results, f, indent=4)
    
    LOGGER.info("Updated /results/unit_test_analysis.json with Top %s", top_n)

def main():
    """Consume repository events and track languages with test folders."""
    pulsar_url = os.getenv("PULSAR_SERVICE_URL", "pulsar://pulsar-broker:6650")
    github_token = os.getenv("GITHUB_TOKEN", "")
    
    LOGGER.info("Connecting to Pulsar at %s...", pulsar_url)
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(
        'persistent://public/default/raw-repositories',
        subscription_name='unit-test-sub',
        consumer_type=pulsar.ConsumerType.Shared
    )
    
    LOGGER.info("Listening for repositories to check for TDD...")
    
    tdd_language_counts = defaultdict(int)
    messages_processed = 0

    while True:
        try:
            msg = consumer.receive()
            repo_data = json.loads(msg.data().decode('utf-8'))
            
            lang = repo_data.get("language")
            contents_url = repo_data.get("contents_url")
            
            # Only bother checking if the repo actually has a defined language
            if lang and contents_url:
                if has_test_folder(contents_url, github_token):
                    tdd_language_counts[lang] += 1
            
            consumer.acknowledge(msg)
            messages_processed += 1
            
            # Save results every 10 messages
            if messages_processed % 10 == 0:
                save_results(tdd_language_counts)

        except Exception as e:
            LOGGER.exception("Failed to process message: %s", e)
            consumer.negative_acknowledge(msg)

if __name__ == "__main__":
    main()
