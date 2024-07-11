import playwright
from playwright.driver 

def launch_browser():
  """Launches a Chromium browser in non-headless mode."""
  p = playwright.chromium.launch(headless=False)
  return p

def main():
  browser = None  # Declare browser variable outside the 'with' block
  try:
    browser = launch_browser()
    page = browser.new_page()
    resolution = {"width": 1920, "height": 1080}
    page.set_viewport_size(resolution)

    url = 'https://manager.masvoz.es/'
    page.goto(url)
    page.get_by_placeholder("Nombre de usuario").fill("bernabe")
    print(page.title())
  except Exception as e:
    # Handle any exceptions that occur
    print(f"Error: {e}")
  finally:
    # Ensure the browser is closed even if there's an exception
    if browser:
      browser.close()

if __name__ == "__main__":
  main()
