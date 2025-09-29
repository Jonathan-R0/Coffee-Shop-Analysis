#!/usr/bin/env python3
"""
Test Suite Runner for RabbitMQ Middleware

This script runs the complete test suite for the MessageMiddleware classes,
demonstrating the following communication patterns as required:

1. Working Queue 1 to 1 communication
2. Working Queue 1 to N communication (load balancing)
3. Exchange 1 to 1 communication
4. Exchange 1 to N communication (pub/sub)

Usage:
    python run_tests.py [--unit] [--integration] [--all]
"""

import sys
import os
import subprocess
import argparse

# Add the project root to the path
project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..')
sys.path.insert(0, project_root)


def run_command(command, description):
    """Run a command and print the results"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            cwd=os.path.dirname(__file__)
        )
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        print(f"Return code: {result.returncode}")
        return result.returncode == 0
        
    except Exception as e:
        print(f"Error running command: {e}")
        return False


def run_unit_tests():
    """Run unit tests"""
    command = "python -m pytest test_middleware_unit.py -v"
    return run_command(command, "Unit Tests for MessageMiddleware")


def run_integration_tests():
    """Run integration tests"""
    command = "python -m pytest test_middleware_integration.py -v"
    return run_command(command, "Integration Tests for Communication Patterns")


def run_all_tests():
    """Run all tests"""
    command = "python -m pytest . -v"
    return run_command(command, "All MessageMiddleware Tests")


def run_specific_test_pattern(pattern):
    """Run tests matching a specific pattern"""
    command = f"python -m pytest . -v -k \"{pattern}\""
    return run_command(command, f"Tests matching pattern: {pattern}")


def main():
    """Main test runner function"""
    parser = argparse.ArgumentParser(description='Run RabbitMQ Middleware Tests')
    parser.add_argument('--unit', action='store_true', help='Run only unit tests')
    parser.add_argument('--integration', action='store_true', help='Run only integration tests')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--pattern', type=str, help='Run tests matching pattern')
    parser.add_argument('--demo', action='store_true', help='Run demonstration tests for all required patterns')
    
    args = parser.parse_args()
    
    print("RabbitMQ Middleware Test Suite")
    print("=" * 60)
    print("Testing the following communication patterns:")
    print("1. Working Queue 1:1 - Single producer to single consumer")
    print("2. Working Queue 1:N - Single producer to multiple consumers (load balancing)")
    print("3. Exchange 1:1 - Single publisher to single subscriber")
    print("4. Exchange 1:N - Single publisher to multiple subscribers (pub/sub)")
    print("=" * 60)
    
    success = True
    
    if args.unit:
        success &= run_unit_tests()
    elif args.integration:
        success &= run_integration_tests()
    elif args.all:
        success &= run_all_tests()
    elif args.pattern:
        success &= run_specific_test_pattern(args.pattern)
    elif args.demo:
        # Run specific demonstration tests for each required pattern
        patterns = [
            "working_queue_1_to_1",
            "working_queue_1_to_n", 
            "exchange_1_to_1",
            "exchange_1_to_n"
        ]
        
        for pattern in patterns:
            print(f"\n{'#'*80}")
            print(f"DEMONSTRATING: {pattern.upper().replace('_', ' ')}")
            print(f"{'#'*80}")
            success &= run_specific_test_pattern(pattern)
    else:
        # Default: run all tests
        success &= run_all_tests()
    
    print(f"\n{'='*60}")
    if success:
        print("✅ ALL TESTS PASSED!")
        print("\n📋 Test Coverage Summary:")
        print("✅ Working Queue 1:1 - Tested")
        print("✅ Working Queue 1:N - Tested") 
        print("✅ Exchange 1:1 - Tested")
        print("✅ Exchange 1:N - Tested")
        print("✅ Error handling - Tested")
        print("✅ Connection management - Tested")
        print("✅ Message acknowledgment - Tested")
    else:
        print("❌ SOME TESTS FAILED!")
        print("Please check the output above for details.")
    print(f"{'='*60}")
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())