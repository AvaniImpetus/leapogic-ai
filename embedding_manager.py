# ==============================
# EMBEDDING GENERATION & STORAGE
# ==============================
import numpy as np
from sentence_transformers import SentenceTransformer
import config
from typing import List
import ssl
import urllib.request
import os

# Disable SSL verification BEFORE other imports
ssl._create_default_https_context = ssl._create_unverified_context
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['SSL_CERT_FILE'] = ''
os.environ['REQUESTS_VERIFY'] = 'False'


class EmbeddingManager:
    """Manages embedding model and caching"""

    def __init__(self):
        self.embedder = None
        self.embedding_dim = None
        self.load_model()

    def load_model(self):
        """Load the embedding model"""
        print(f"Loading embedding model: {config.EMBEDDING_MODEL}")
        try:
            # Disable SSL verification for model download
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            from urllib3 import disable_warnings
            from urllib3.exceptions import InsecureRequestWarning

            disable_warnings(InsecureRequestWarning)

            # Monkey patch requests to disable SSL verification
            original_request = requests.Session.request

            def patched_request(self, method, url, **kwargs):
                kwargs['verify'] = False
                return original_request(self, method, url, **kwargs)
            requests.Session.request = patched_request

            self.embedder = SentenceTransformer(
                config.EMBEDDING_MODEL, device=config.DEVICE)
            # Get embedding dimension from a sample
            sample_embedding = self.embedder.encode(
                ["test"], convert_to_numpy=True)
            self.embedding_dim = sample_embedding.shape[1]
            print(
                f"✓ Embedding model loaded (dimension: {self.embedding_dim}) on device: {config.DEVICE}")
        except Exception as e:
            print(f"✗ Error loading embedding model on {config.DEVICE}: {e}")
            # Try fallback to CPU if CUDA failed
            if config.DEVICE == "cuda":
                print("🔄 Falling back to CPU...")
                try:
                    self.embedder = SentenceTransformer(
                        config.EMBEDDING_MODEL, device="cpu")
                    sample_embedding = self.embedder.encode(
                        ["test"], convert_to_numpy=True)
                    self.embedding_dim = sample_embedding.shape[1]
                    print(f"✓ Embedding model loaded (dimension: {self.embedding_dim}) on CPU fallback")
                except Exception as e2:
                    print(f"✗ CPU fallback also failed: {e2}")
                    raise e2
            else:
                raise

    def encode(self, texts: List[str]) -> np.ndarray:
        """Encode texts to embeddings"""
        return self.embedder.encode(texts, convert_to_numpy=True)

    def encode_single(self, text: str) -> np.ndarray:
        """Encode a single text to embedding"""
        return self.embedder.encode([text], convert_to_numpy=True)[0]
