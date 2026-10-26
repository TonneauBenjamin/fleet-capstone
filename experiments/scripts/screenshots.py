"""
Screenshots of the Spark and Airflow web UIs with a headless browser, from inside the compose network.
Usage: python experiments/scripts/screenshots.py OUT_DIR name=url [name=url ...]
Airflow pages need AIRFLOW_PASSWORD (user admin).
url = query:<host:port>:<query name> -> statistics page of that streaming query in the Spark UI.
"""
import os
import sys

from playwright.sync_api import sync_playwright

out = sys.argv[1]
pages = [a.split("=", 1) for a in sys.argv[2:]]
os.makedirs(out, exist_ok=True)

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1600, "height": 1000})
    if any("airflow:8080" in url for _, url in pages):
        page.goto("http://airflow:8080/login/")
        page.fill("#username", "admin")
        page.fill("#password", os.environ["AIRFLOW_PASSWORD"])
        page.click("input[type=submit]")
        page.wait_for_load_state("networkidle")
    for name, url in pages:
        if url.startswith("query:"):
            _, host, port, query = url.split(":")
            page.goto(f"http://{host}:{port}/StreamingQuery/")
            page.click(f"tr:has(td:text-is('{query}')) a")
        else:
            page.goto(url)
        page.wait_for_timeout(3000)
        page.screenshot(path=f"{out}/{name}.png", full_page=True)
        print("saved", f"{out}/{name}.png")
    browser.close()
