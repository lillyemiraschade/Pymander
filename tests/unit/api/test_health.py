"""Tests for API health endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pymander.api.app import create_app


@pytest.fixture
def client():
    app = create_app()
    return TestClient(app)


class TestHealthEndpoints:
    def test_health(self, client):
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_ready(self, client):
        response = client.get("/api/v1/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}
