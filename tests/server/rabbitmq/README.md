# RabbitMQ Middleware Test Suite

This test suite provides comprehensive unit and integration tests for the `MessageMiddlewareQueue` and `MessageMiddlewareExchange` classes, covering all required communication patterns.

## 📁 Test Structure

```
tests/server/rabbitmq/
├── __init__.py                      # Package initialization
├── conftest.py                      # Pytest configuration
├── test_middleware_unit.py          # Unit tests for both classes
├── test_middleware_integration.py   # Integration tests for communication patterns
├── run_tests.py                     # Test runner script
└── README.md                        # This file
```

## 🧪 Test Coverage

### Required Communication Patterns Tested

✅ **Working Queue 1:1** - Single producer to single consumer  
✅ **Working Queue 1:N** - Single producer to multiple consumers (load balancing)  
✅ **Exchange 1:1** - Single publisher to single subscriber  
✅ **Exchange 1:N** - Single publisher to multiple subscribers (pub/sub)  

### Unit Tests (`test_middleware_unit.py`)

**MessageMiddlewareQueue Tests:**
- ✅ Successful initialization and connection setup
- ✅ Connection error handling during initialization
- ✅ Message sending with various scenarios
- ✅ Message consumption start/stop
- ✅ Error handling for disconnected state
- ✅ Queue deletion and connection closure
- ✅ Callback wrapper functionality with success/error cases

**MessageMiddlewareExchange Tests:**
- ✅ Initialization with single and multiple routing keys
- ✅ Connection error handling during initialization  
- ✅ Message publishing with custom and default routing keys
- ✅ Message consumption with temporary queue binding
- ✅ Exchange deletion and connection management
- ✅ Callback wrapper functionality with acknowledgments

### Integration Tests (`test_middleware_integration.py`)

**Working Queue Communication:**
- ✅ **1:1 Pattern**: Single producer sends to single consumer
- ✅ **1:N Pattern**: Single producer distributes messages among multiple consumers (load balancing)

**Exchange Communication:**
- ✅ **1:1 Pattern**: Single publisher sends to single subscriber
- ✅ **1:N Pattern**: Single publisher broadcasts to multiple subscribers (pub/sub)
- ✅ **Multiple Routing Keys**: Exchange handling different routing patterns

## 🚀 Running the Tests

### Prerequisites

```bash
pip install pytest pika
```

### Quick Start

```bash
# Run all tests
python run_tests.py --all

# Run only unit tests
python run_tests.py --unit

# Run only integration tests  
python run_tests.py --integration

# Run demonstration of all required patterns
python run_tests.py --demo

# Run specific test pattern
python run_tests.py --pattern "working_queue_1_to_1"
```

### Using pytest directly

```bash
# Run all tests with verbose output
pytest . -v

# Run only unit tests
pytest test_middleware_unit.py -v

# Run only integration tests
pytest test_middleware_integration.py -v

# Run tests matching a pattern
pytest . -v -k "1_to_1"
```

## 🔍 Test Design Principles

### Unit Testing Best Practices Applied

1. **Isolation**: Each test is independent and doesn't rely on external services
2. **Mocking**: Uses `unittest.mock` to mock RabbitMQ connections and channels
3. **Edge Cases**: Tests both success and failure scenarios
4. **Error Handling**: Verifies custom exceptions are raised appropriately
5. **State Verification**: Checks that internal state is correctly maintained

### Integration Testing Approach

1. **Pattern Verification**: Tests actual communication patterns required by the specification
2. **Concurrency**: Uses threading to simulate real-world producer/consumer scenarios
3. **Message Verification**: Confirms messages are delivered correctly and completely
4. **Load Distribution**: Verifies load balancing in working queue pattern
5. **Broadcast Verification**: Confirms pub/sub behavior in exchange pattern

## 📊 Expected Test Results

When all tests pass, you should see:

```
✅ ALL TESTS PASSED!

📋 Test Coverage Summary:
✅ Working Queue 1:1 - Tested
✅ Working Queue 1:N - Tested
✅ Exchange 1:1 - Tested  
✅ Exchange 1:N - Tested
✅ Error handling - Tested
✅ Connection management - Tested
✅ Message acknowledgment - Tested
```

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure the project root is in your Python path
2. **Missing Dependencies**: Install `pytest` and `pika` packages
3. **Mock Failures**: Unit tests use mocks, so they don't require actual RabbitMQ

### Test Debugging

To debug specific tests:

```bash
# Run with maximum verbosity
pytest test_middleware_unit.py::TestMessageMiddlewareQueue::test_send_message_success -vvv

# Run with pdb debugger
pytest --pdb test_middleware_unit.py::TestMessageMiddlewareQueue::test_send_message_success
```

## 🔧 Extending the Tests

To add new test cases:

1. **Unit Tests**: Add methods to existing test classes in `test_middleware_unit.py`
2. **Integration Tests**: Add new communication pattern tests to `test_middleware_integration.py`
3. **Test Markers**: Use `@pytest.mark.unit` or `@pytest.mark.integration` for categorization

## 📝 Notes

- All tests use mocking to avoid requiring an actual RabbitMQ instance
- Integration tests simulate the behavior of real RabbitMQ communication patterns
- The test suite is designed to verify the middleware interface compliance
- Error scenarios are thoroughly tested to ensure robust error handling
- Threading is used in integration tests to simulate concurrent producers/consumers