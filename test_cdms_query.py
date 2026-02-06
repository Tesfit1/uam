"""
Test script to verify CDMS VQL query format and authentication
"""
import os
from dotenv import load_dotenv
import requests

load_dotenv()

CDMS_BASE_URL = os.getenv("BASE_URL")
CDMS_API_VERSION = os.getenv("API_VERSION")
CDMS_SESSION_FILE = "CDMSsession_id.txt"

def test_cdms_session():
    """Test if CDMS session is valid."""
    print("=" * 60)
    print("CDMS Session & Query Test")
    print("=" * 60)

    # Check session file
    if not os.path.exists(CDMS_SESSION_FILE):
        print(f"❌ Session file not found: {CDMS_SESSION_FILE}")
        print("   Run: python CDMSAuth.py")
        return False

    with open(CDMS_SESSION_FILE) as f:
        session_id = f.read().strip()

    if not session_id:
        print(f"❌ Session file is empty: {CDMS_SESSION_FILE}")
        print("   Run: python CDMSAuth.py")
        return False

    print(f"✅ Session ID loaded: {session_id[:20]}...")

    # Test simple query
    test_query = "SELECT id, name__v FROM study__v LIMIT 5"

    url = f"{CDMS_BASE_URL}/api/{CDMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": f"Bearer {session_id}"
    }
    payload = {"q": test_query}

    print(f"\n🔍 Testing query:")
    print(f"   URL: {url}")
    print(f"   Query: {test_query}")

    try:
        response = requests.post(url, data=payload, headers=headers)

        print(f"\n📊 Response:")
        print(f"   Status Code: {response.status_code}")

        if response.status_code == 200:
            json_response = response.json()

            if "errors" in json_response:
                print(f"   ❌ API Errors: {json_response['errors']}")
                return False

            data = json_response.get("data", [])
            print(f"   ✅ Success! Found {len(data)} studies")

            if data:
                print(f"\n   Sample studies:")
                for i, study in enumerate(data[:3], 1):
                    print(f"     {i}. {study.get('name__v')} (ID: {study.get('id')})")

            return True

        elif response.status_code == 401:
            json_response = response.json()
            print(f"   ❌ Authentication failed")
            if "errors" in json_response:
                for error in json_response["errors"]:
                    print(f"      - {error.get('type')}: {error.get('message')}")
            print(f"\n   💡 Solution: Run python CDMSAuth.py to refresh session")
            return False

        else:
            print(f"   ❌ Unexpected status code")
            print(f"   Response: {response.text[:200]}")
            return False

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def test_or_query():
    """Test OR query with multiple study names."""
    print("\n" + "=" * 60)
    print("Testing OR Query")
    print("=" * 60)

    with open(CDMS_SESSION_FILE) as f:
        session_id = f.read().strip()

    # Test with sample study names
    test_names = ["2111-2111", "497575_Tosca_V2V", "NonExistentStudy_123"]

    # Build query with escaping
    escaped_names = [name.replace("'", "\\'") for name in test_names]
    conditions = " OR ".join([f"name__v = '{name}'" for name in escaped_names])
    query = f"SELECT name__v FROM study__v WHERE ({conditions})"

    url = f"{CDMS_BASE_URL}/api/{CDMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": f"Bearer {session_id}"
    }
    payload = {"q": query}

    print(f"\n🔍 Testing OR query:")
    print(f"   Query: {query}")

    try:
        response = requests.post(url, data=payload, headers=headers)

        print(f"\n📊 Response:")
        print(f"   Status Code: {response.status_code}")

        if response.status_code == 200:
            json_response = response.json()

            if "errors" in json_response:
                print(f"   ❌ Query Errors: {json_response['errors']}")
                return False

            data = json_response.get("data", [])
            found_names = [s.get("name__v") for s in data]

            print(f"   ✅ Query successful!")
            print(f"   Searched for: {test_names}")
            print(f"   Found: {found_names}")
            print(f"   Not found: {[n for n in test_names if n not in found_names]}")

            return True

        elif response.status_code == 401:
            print(f"   ❌ Session expired")
            print(f"   💡 Solution: Run python CDMSAuth.py")
            return False

        else:
            print(f"   ❌ Error {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

if __name__ == "__main__":
    result1 = test_cdms_session()

    if result1:
        result2 = test_or_query()

        if result2:
            print("\n" + "=" * 60)
            print("✅ All tests passed!")
            print("=" * 60)
            print("\nYour CDMS query configuration is working correctly.")
            print("You can now run: python ClinicalStudyList.py")
        else:
            print("\n" + "=" * 60)
            print("⚠️ OR query test failed")
            print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ Session test failed")
        print("=" * 60)
        print("\nPlease fix the session issue before proceeding.")
