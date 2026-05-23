import json
import os
import time
from collections import defaultdict
import pulsar
import requests

# Q1: Top 10 programming languages by number of projects
# Q2: Top 10 most frequently updated projects (by commit count)

def get_commit_count(commits_url: str, token: str) -> int:
    """
    Fetches the total commit count for a repository using the GitHub API pagination header trick.
    """
    if not commits_url:
        return 0
        
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        # We request only 1 item per page. GitHub returns a 'Link' header telling us the last page.
        # The last page number equals the total number of commits!
        response = requests.get(f"{commits_url}?per_page=1", headers=headers, timeout=10)
        
        if response.status_code == 200:
            if "Link" in response.headers:
                # Parse the Link header to find the 'last' page
                links = response.headers["Link"].split(",")
                for link in links:
                    if 'rel="last"' in link:
                        # Extract the page number from the URL
                        last_page_url = link[link.find("<")+1:link.find(">")]
                        page_num = int(last_page_url.split("page=")[-1].split("&")[0])
                        return page_num
            # If there's no Link header, there's only 1 page (so 1 commit)
            return len(response.json())
            
        elif response.status_code == 403:
            print("[consumer-language-stats] Rate limit hit on commits API. Pausing...")
            time.sleep(60) # Simple backoff
            return 0
    except Exception as e:
        print(f"[consumer-language-stats] Error fetching commits: {e}")
        
    return 0

def save_results(language_counts, top_projects):
    top_n = int(os.getenv("TOP_N", "10"))
    
    # Sort Q1
    sorted_languages = sorted(language_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    # Sort Q2
    sorted_projects = sorted(top_projects, key=lambda x: x['commits'], reverse=True)[:top_n]
    
    results = {
        "Q1_top_languages": sorted_languages,
        "Q2_top_projects_by_commits": sorted_projects
    }
    
    # Save to the mounted volume
    os.makedirs("/results", exist_ok=True)
    with open("/results/language_stats.json", "w") as f:
        json.dump(results, f, indent=4)
    
    print(f"[consumer-language-stats] Updated /results/language_stats.json with Top {top_n}")

def main():
    pulsar_url = os.getenv("PULSAR_SERVICE_URL", "pulsar://pulsar-broker:6650")
    github_token = os.getenv("GITHUB_TOKEN", "")
    
    print(f"[consumer-language-stats] Connecting to Pulsar at {pulsar_url}...")
    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(
        'persistent://public/default/raw-repositories',
        subscription_name='language-stats-sub',
        consumer_type=pulsar.ConsumerType.Shared
    )
    
    print("[consumer-language-stats] Listening for repositories...")
    
    language_counts = defaultdict(int)
    top_projects = []
    messages_processed = 0

    while True:
        try:
            msg = consumer.receive()
            repo_data = json.loads(msg.data().decode('utf-8'))
            
            # --- Q1 Logic ---
            lang = repo_data.get("language")
            if lang:
                language_counts[lang] += 1
                
            # --- Q2 Logic ---
            # To prevent rate-limiting ourselves to death, we only fetch commits for popular repos
            # (In a real massive cluster, you'd fetch all of them, but we must protect our token)
            commit_count = get_commit_count(repo_data.get("commits_url"), github_token)
            
            top_projects.append({
                "name": repo_data.get("full_name"),
                "language": lang,
                "commits": commit_count
            })
            
            # Keep the list small in memory
            top_projects = sorted(top_projects, key=lambda x: x['commits'], reverse=True)[:50]
            
            consumer.acknowledge(msg)
            messages_processed += 1
            
            # Save results every 10 messages so we can see it updating live
            if messages_processed % 10 == 0:
                save_results(language_counts, top_projects)

        except Exception as e:
            print(f"[consumer-language-stats] Failed to process message: {e}")
            consumer.negative_acknowledge(msg)

if __name__ == "__main__":
    main()
