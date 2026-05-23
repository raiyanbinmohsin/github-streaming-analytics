import json
import logging
import os
import time
from collections import defaultdict
import pulsar
import requests

# Q4: Top 10 languages combining TDD and DevOps/CI practices

TEST_INDICATORS = {"test", "tests", "spec", "specs", "__tests__", "testing"}
CI_INDICATORS = {".github", ".travis.yml", ".gitlab-ci.yml", ".circleci", "jenkinsfile"}
LOGGER = logging.getLogger("consumer.ci_devops_analysis")

def check_practices(contents_url: str, token: str) -> tuple[bool, bool]:
    """
    Checks the root directory for both TDD folders and CI/CD configuration files.
    Returns a tuple: (has_tdd, has_ci)
    """
    if not contents_url:
        return False, False
        
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    has_tdd = False
    has_ci = False

    try:
        response = requests.get(contents_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            contents = response.json()
            if isinstance(contents, list):
                for item in contents:
                    name = item.get("name", "").lower()
                    if item.get("type") == "dir" and name in TEST_INDICATORS:
                        has_tdd = True
                    if name in CI_INDICATORS:
                        has_ci = True
                        
                    # Early exit if we found both
                    if has_tdd and has_ci:
                        return True, True
                        
            return has_tdd, has_ci
            
        elif response.status_code == 403:
            LOGGER.warning("Rate limit hit on contents API. Pausing...")
            time.sleep(60) 
            return False, False
            
    except Exception as e:
        LOGGER.warning("Error fetching contents: %s", e)
        
    return has_tdd, has_ci

def save_results(combined_practices_counts):
    """Persist Q4 language counts to the shared results JSON file."""
    top_n = int(os.getenv("TOP_N", "10"))
    
    sorted_languages = sorted(combined_practices_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    results = {
        "Q4_top_languages_tdd_and_ci": sorted_languages
    }
    
    os.makedirs("/results", exist_ok=True)
    with open("/results/ci_devops_analysis.json", "w") as f:
        json.dump(results, f, indent=4)
    
    LOGGER.info("Updated /results/ci_devops_analysis.json with Top %s", top_n)

def main():
    """Consume repository events and track languages using TDD plus CI/CD."""
    pulsar_url = os.getenv("PULSAR_SERVICE_URL", "pulsar://pulsar-broker:6650")
    github_token = os.getenv("GITHUB_TOKEN", "")
    
    LOGGER.info("Connecting to Pulsar at %s...", pulsar_url)
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(
        'persistent://public/default/raw-repositories',
        subscription_name='ci-devops-sub',
        consumer_type=pulsar.ConsumerType.Shared
    )
    
    LOGGER.info("Listening for repositories to check for TDD + CI/CD...")
    
    combined_practices_counts = defaultdict(int)
    messages_processed = 0

    while True:
        try:
            msg = consumer.receive()
            repo_data = json.loads(msg.data().decode('utf-8'))
            
            lang = repo_data.get("language")
            contents_url = repo_data.get("contents_url")
            
            if lang and contents_url:
                has_tdd, has_ci = check_practices(contents_url, github_token)
                if has_tdd and has_ci:
                    combined_practices_counts[lang] += 1
            
            consumer.acknowledge(msg)
            messages_processed += 1
            
            if messages_processed % 10 == 0:
                save_results(combined_practices_counts)

        except Exception as e:
            LOGGER.exception("Failed to process message: %s", e)
            consumer.negative_acknowledge(msg)

if __name__ == "__main__":
    main()
