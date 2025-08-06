#!/usr/bin/env python3
"""
Test script to verify Apache Polaris and Iceberg services are working
Run this after starting docker-compose services
"""

import subprocess
import time
import sys
from typing import Dict, Any


def test_docker_services() -> Dict[str, Any]:
    """Test if Docker Compose services are running"""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✅ Docker Compose is running")
            return {"docker_compose": True, "output": result.stdout}
        else:
            print("❌ Docker Compose issues detected")
            return {"docker_compose": False, "error": result.stderr}
            
    except Exception as e:
        print(f"❌ Error checking Docker Compose: {e}")
        return {"docker_compose": False, "error": str(e)}


def test_polaris_service() -> Dict[str, Any]:
    """Test Apache Polaris REST API"""
    try:
        # Simple HTTP check without external dependencies
        import urllib.request
        import json
        
        # Test management endpoint
        management_url = "http://localhost:8181/management/v1/config"
        
        with urllib.request.urlopen(management_url, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode())
                print("✅ Apache Polaris is responding")
                return {
                    "polaris": True, 
                    "management_url": management_url,
                    "config": data
                }
            else:
                print(f"❌ Polaris returned status {response.status}")
                return {"polaris": False, "status": response.status}
                
    except Exception as e:
        print(f"❌ Error connecting to Polaris: {e}")
        return {"polaris": False, "error": str(e)}


def test_iceberg_service() -> Dict[str, Any]:
    """Test Iceberg service endpoint"""
    try:
        import urllib.request
        
        # Test Iceberg health endpoint
        iceberg_url = "http://localhost:8080/health"
        
        with urllib.request.urlopen(iceberg_url, timeout=5) as response:
            if response.status == 200:
                print("✅ Iceberg service is responding")
                return {"iceberg": True, "health_url": iceberg_url}
            else:
                print(f"❌ Iceberg returned status {response.status}")
                return {"iceberg": False, "status": response.status}
                
    except Exception as e:
        print(f"❌ Error connecting to Iceberg: {e}")
        return {"iceberg": False, "error": str(e)}


def main():
    """Run all service tests"""
    print("🧪 Testing Apache Polaris + Iceberg Services")
    print("=" * 60)
    
    # Check if services should be started
    docker_status = test_docker_services()
    
    if not docker_status.get("docker_compose", False):
        print("\n💡 To start services, run:")
        print("   docker-compose up -d")
        print("\n⏳ Then wait about 30 seconds for services to initialize...")
        return
    
    print("\n⏳ Waiting for services to initialize...")
    time.sleep(5)  # Give services time to start
    
    # Test individual services
    polaris_status = test_polaris_service()
    iceberg_status = test_iceberg_service()
    
    print(f"\n📊 Service Status Summary:")
    print(f"   🐳 Docker Compose: {'✅' if docker_status.get('docker_compose') else '❌'}")
    print(f"   🌟 Apache Polaris: {'✅' if polaris_status.get('polaris') else '❌'}")  
    print(f"   📊 Iceberg Service: {'✅' if iceberg_status.get('iceberg') else '❌'}")
    
    if all([
        docker_status.get("docker_compose"),
        polaris_status.get("polaris"),
        iceberg_status.get("iceberg")
    ]):
        print(f"\n🎉 All services are running successfully!")
        print(f"\n🔗 Service URLs:")
        print(f"   • Apache Polaris: http://localhost:8181")
        print(f"   • Iceberg Service: http://localhost:8080")
        print(f"\n📚 Next Steps:")
        print(f"   1. Run: python -c \"from config.iceberg_spatial_config import print_system_status; print_system_status()\"")
        print(f"   2. Install local Python deps: bash setup-local-python.sh")
        print(f"   3. Open notebooks/ folder for examples")
    else:
        print(f"\n⚠️  Some services are not ready. Check docker-compose logs:")
        print(f"   docker-compose logs")


if __name__ == "__main__":
    main()
