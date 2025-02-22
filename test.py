import requests
import time

BASE_URL = "http://localhost:8080"

id = 0

def test_set_key():
    """Test setting a key-value pair with expiry"""
    key = "test_key"
    value = "test_value"
    expiry = int(time.time()) + 5  # Set expiry 5 seconds from now
    print(expiry, ' expiry ')
    url = f"{BASE_URL}/rpc/set?key={key}&value={value}&expiry={expiry}"
    response = requests.put(url)

    if response.status_code == 200:
        print("[✅] Key set successfully:", response.json())
    else:
        print("[❌] Failed to set key:", response.text)


def test_get_key():
    """Test retrieving the key"""
    key = "test_key"
    url = f"{BASE_URL}/rpc/get?key={key}"
    response = requests.get(url)

    if response.status_code == 200:
        print("[✅] Key retrieved:", response.json())
    else:
        print("[❌] Key retrieval failed:", response.text)


def test_delete_key():
    """Test deleting a key"""
    key = "test_key"
    url = f"{BASE_URL}/rpc/delete?key={key}"
    response = requests.delete(url)

    if response.status_code == 200:
        print("[✅] Key deleted successfully:", response.json())
    else:
        print("[❌] Key deletion failed:", response.text)

    # Verify key has been deleted
    print("\n[⏳] Verifying key deletion:")
    test_get_key()  # Should fail since the key was deleted


def test_expiry_behavior():
    """Test that the key expires after 5 seconds"""
    print("\n[⏳] Fetching key immediately:")
    test_get_key()  # Should succeed

    print("\n[⏳] Waiting for 7 seconds...")
    time.sleep(7)  # Wait for expiry

    print("\n[⏳] Fetching key after expiry:")
    test_get_key()  # Should fail


if __name__ == "__main__":
    test_set_key()
    test_expiry_behavior()

    print("\n[🗑️] Testing key deletion:")
    test_set_key()  # Reset key for delete test
    test_delete_key()
