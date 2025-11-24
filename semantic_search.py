# ==============================
# SEMANTIC SEARCH & RETRIEVAL
# ==============================
import sqlite3
from typing import List, Tuple

import numpy as np

import config
import utilities as utils
from embedding_manager import EmbeddingManager


class SemanticSearcher:
    """Performs semantic search on the vector database"""

    def __init__(self, embedding_manager: EmbeddingManager, db_file: str = None):
        self.embedding_manager = embedding_manager
        self.db_file = db_file or config.VECTOR_DB_FILE

    def search(self, query: str, top_k: int = config.TOP_K_RETRIEVAL, file_filter: str = None) -> List[Tuple[str, str, float]]:
        """
        Search for relevant chunks using semantic similarity
        Returns list of (chunk_content, file_name, similarity_score)
        """
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Encode query
        query_embedding = self.embedding_manager.encode_single(query)

        # Retrieve all chunks
        cursor.execute("""
            SELECT c.id, c.chunk_content, d.file_name, c.embedding, c.embedding_dim
            FROM chunks c
            JOIN documents d ON c.doc_id = d.id
        """)

        results = cursor.fetchall()
        conn.close()

        if not results:
            return []

        # Calculate similarity scores using cosine similarity
        similarities = []
        for chunk_id, chunk_content, file_name, embedding_blob, embedding_dim in results:
            chunk_embedding = utils.blob_to_embedding(
                embedding_blob, embedding_dim)

            # Flatten embeddings to 1D for dot product
            query_flat = query_embedding.flatten()
            chunk_flat = chunk_embedding.flatten()

            # Cosine similarity
            dot_product = np.dot(query_flat, chunk_flat)
            norm_query = np.linalg.norm(query_flat)
            norm_chunk = np.linalg.norm(chunk_flat)

            similarity = dot_product / (norm_query * norm_chunk + 1e-10)
            similarities.append((chunk_content, file_name, float(similarity)))

        # Sort by similarity and return top-k
        similarities.sort(key=lambda x: x[2], reverse=True)
        if len(similarities) < top_k:
            top_k = len(similarities)

        # Group by file_name and take the chunk with max similarity for unique files
        from collections import defaultdict
        file_max_sim = defaultdict(lambda: {'sim': 0, 'chunk': ''})
        for chunk_content, file_name, sim in similarities:
            if sim > file_max_sim[file_name]['sim']:
                file_max_sim[file_name] = {'sim': sim, 'chunk': chunk_content}

        # Create unique list sorted by similarity
        unique_similarities = [(data['chunk'], file_name, data['sim'])
                               for file_name, data in file_max_sim.items()]
        unique_similarities.sort(key=lambda x: x[2], reverse=True)

        # Return top-k unique files
        if len(unique_similarities) < top_k:
            top_k = len(unique_similarities)

        # Filter by file_filter if provided
        if file_filter:
            unique_similarities = [
                item for item in unique_similarities if item[1] == file_filter]
            if len(unique_similarities) < top_k:
                top_k = len(unique_similarities)

        return unique_similarities[:top_k]
