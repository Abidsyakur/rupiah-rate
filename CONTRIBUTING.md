# Contributing to Rupiah Exchange Rate Intelligence

We welcome contributions! Please follow these guidelines.

## Getting Started

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes
4. Write tests for new functionality
5. Commit with clear messages
6. Push and create a Pull Request

## Code Standards

- Follow PEP 8 style guide
- Use type hints
- Write docstrings for public functions
- Run `black`, `flake8`, and `mypy` before committing

```bash
black src/
flake8 src/
mypy src/
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/
```

## Pull Request Process

1. Update documentation if needed
2. Add tests for new features
3. Ensure all tests pass
4. Request review from maintainers
5. Address feedback and push updates

## Questions?

Open an issue or contact the maintainers.
