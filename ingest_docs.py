# ==============================
# DOCUMENTATION LOADING
# ==============================

import os
import requests
import sqlite3
from typing import List

import config
import utilities as utils
from embedding_manager import EmbeddingManager


class DocumentIngestion:
    """Loads markdown files and manages vector database"""

    def __init__(self, embedding_manager: EmbeddingManager, docs_folder: str = None, db_file: str = None):
        self.docs_folder = docs_folder or config.DOCS_FOLDER
        self.db_file = db_file or config.VECTOR_DB_FILE
        self.embedding_manager = embedding_manager
        self.initialize_vector_db()

    def initialize_vector_db(self):
        """Initialize the vector database"""
        self.initialize_vector_db_file()

    def load_markdown_to_db(self, overwrite_existing: bool = False) -> int:
        """Fetch all markdown files and load them into the vector database

        Args:
            overwrite_existing: If True, overwrite existing entries for files already in the database
        """
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        md_files = self.fetch_markdown_files(self.docs_folder)
        if not md_files:
            print("⚠ No markdown files found in docs folder")
            conn.close()
            return 0

        print(f"\n📚 Found {len(md_files)} markdown files")
        print("=" * 60)

        total_chunks_added = 0

        for file_path in md_files:
            try:
                # Check if file already exists in database
                cursor.execute(
                    "SELECT id FROM documents WHERE file_path = ?", (file_path,))
                existing = cursor.fetchone()
                if existing:
                    if not overwrite_existing:
                        print(
                            f"  ⊘ Skipped (already in DB): {os.path.basename(file_path)}")
                        continue
                    else:
                        cursor.execute(
                            "DELETE FROM documents WHERE id = ?", (existing[0],))
                        print(
                            f"  ↻ Overwriting: {os.path.basename(file_path)}")

                # Read file content
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                if not content.strip():
                    print(f"  ⚠ Empty file: {os.path.basename(file_path)}")
                    continue

                # Chunk the content
                chunks = self.chunk_text(content, config.CHUNK_SIZE)
                if not chunks:
                    print(
                        f"  ⚠ No chunks created: {os.path.basename(file_path)}")
                    continue

                # Generate embeddings for all chunks
                print(
                    f"  📝 {os.path.basename(file_path)}: Creating {len(chunks)} embeddings...", end='', flush=True)
                try:
                    chunk_embeddings = self.embedding_manager.encode(chunks)
                    if chunk_embeddings.shape[0] != len(chunks):
                        print(f" ✗ Embedding shape mismatch")
                        continue
                except Exception as e:
                    print(f" ✗ Error creating embeddings: {e}")
                    continue

                # Insert document
                file_name = os.path.basename(file_path)
                cursor.execute("""
                    INSERT INTO documents (file_path, file_name, original_content, chunk_count)
                    VALUES (?, ?, ?, ?)
                """, (file_path, file_name, content, len(chunks)))

                doc_id = cursor.lastrowid

                # Insert chunks with embeddings
                for chunk_index, chunk in enumerate(chunks):
                    embedding = chunk_embeddings[chunk_index]
                    embedding_blob = utils.embedding_to_blob(embedding)

                    cursor.execute("""
                        INSERT INTO chunks (doc_id, chunk_index, chunk_content, embedding, embedding_dim)
                        VALUES (?, ?, ?, ?, ?)
                    """, (doc_id, chunk_index, chunk, embedding_blob, self.embedding_manager.embedding_dim))

                conn.commit()
                total_chunks_added += len(chunks)
                print(f" ✓ Stored {len(chunks)} chunks")

            except Exception as e:
                print(f"  ✗ Error loading {os.path.basename(file_path)}: {e}")
                import traceback
                traceback.print_exc()

        conn.close()
        print("=" * 60)
        if overwrite_existing:
            print(
                f"✓ Documentation fully reloaded! All existing entries overwritten. ({total_chunks_added} chunks indexed)\n")
        else:
            print(
                f"✓ Documentation reloaded! New files added, existing skipped. ({total_chunks_added} chunks indexed)\n")
        return total_chunks_added

    # ==============================
    # DATABASE INITIALIZATION
    # ==============================
    def initialize_vector_db_file(self):
        """Create SQLite database schema for vector embeddings"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # Table for documents metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT UNIQUE NOT NULL,
                file_name TEXT NOT NULL,
                original_content TEXT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                chunk_count INTEGER DEFAULT 0
            )
        """)

        # Table for document chunks and their embeddings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id INTEGER NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_content TEXT NOT NULL,
                embedding BLOB NOT NULL,
                embedding_dim INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(doc_id) REFERENCES documents(id) ON DELETE CASCADE,
                UNIQUE(doc_id, chunk_index)
            )
        """)

        # Create index for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_id ON chunks(doc_id)
        """)

        conn.commit()
        conn.close()
        print(f"✓ Vector database initialized: {self.db_file}")

    # ==============================
    # MARKDOWN FILE PROCESSING
    # ==============================
    def fetch_markdown_files(self, folder: str) -> List[str]:
        """Recursively fetch all markdown files from the specified folder"""
        md_files = []
        folder = folder or self.docs_folder
        if not os.path.exists(folder):
            print(f"⚠ Documentation folder not found: {folder}")
            return md_files

        for root, dirs, files in os.walk(folder):
            for file in files:
                if file.endswith('.md'):
                    file_path = os.path.join(root, file)
                    md_files.append(file_path)

        return sorted(md_files)

    def chunk_text(self, text: str, chunk_size: int = config.CHUNK_SIZE) -> List[str]:
        """Split text into chunks by word count with overlap"""
        words = text.split()
        chunks = []

        # Create chunks with 10% overlap
        overlap = max(1, chunk_size // 10)
        step = chunk_size - overlap

        for i in range(0, len(words), step):
            chunk = " ".join(words[i:i + chunk_size])
            if chunk.strip():
                chunks.append(chunk)

            # Stop if we're at the end
            if i + chunk_size >= len(words):
                break

        return chunks if chunks else [text]
