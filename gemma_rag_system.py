"""
Gemma RAG System with Vector Database Support
Generates answers using Gemma AI model augmented with knowledge base retrieved from markdown files.
Stores embeddings in vector.db for efficient retrieval.
"""

from semantic_search import SemanticSearcher
from ingest_docs import DocumentIngestion
from embedding_manager import EmbeddingManager
import config
from typing import Union, List
import google.generativeai as genai
import ssl
import sqlite3
from datetime import datetime
import os
import requests

# Disable SSL verification globally BEFORE importing google.generativeai
ssl._create_default_https_context = ssl._create_unverified_context
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['SSL_CERT_FILE'] = ''

# Force google.generativeai to use REST instead of gRPC to avoid SSL issues
os.environ['GOOGLE_API_USE_CLIENT_CERTIFICATE'] = 'false'


# Configure Gemma API to use REST transport
genai.configure(api_key=config.API_KEY, transport='rest')

# ==============================
# ANSWER GENERATION
# ==============================


class GemmaAnswerGenerator:
    """Generates answers using Gemma model"""

    def __init__(self):
        self.model = genai.GenerativeModel(config.GEMMA_MODEL)
        print(f"✓ Gemma model configured: {config.GEMMA_MODEL}\n")

    def generate_answer(self, question: str, context: str, conversation_context: str = "") -> str:
        """Generate answer using Gemma model with context"""
        # Include conversation context if available
        context_with_history = context
        if conversation_context:
            context_with_history = f"{conversation_context}\n\n{context}" if context else conversation_context

        if context_with_history.strip():
            prompt = f"""You are a helpful assistant with access to a knowledge base.
Based on the following knowledge base content, provide a clear and accurate answer to the question.
If the context doesn't contain relevant information, say so honestly.

{context_with_history}

ANSWER:"""
        else:
            prompt = f"""Answer the following question:

QUESTION: {question}

ANSWER:"""

        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error generating answer: {e}"


# ==============================
# RAG SYSTEM ORCHESTRATOR
# ==============================
class GemmaRAGSystem:
    """Complete RAG system combining all components"""

    def __init__(self, docs_folder: str = None, db_file: str = None):
        print("🚀 Initializing Gemma RAG System")
        print("=" * 60)

        # Initialize components
        self.embedding_manager = EmbeddingManager()
        self.kb_loader = DocumentIngestion(
            self.embedding_manager, docs_folder, db_file)
        self.searcher = SemanticSearcher(
            self.embedding_manager, db_file or config.VECTOR_DB_FILE)
        self.answer_generator = GemmaAnswerGenerator()

        # Load knowledge base if documents exist
        self.load_knowledge_base(overwrite_existing=True)

    def load_knowledge_base(self, overwrite_existing: bool = False):
        """Load markdown files into vector database"""
        chunks_loaded = self.kb_loader.load_markdown_to_db(
            overwrite_existing=overwrite_existing)
        if chunks_loaded == 0:
            if overwrite_existing:
                print("⚠ No documents reloaded. Database remains unchanged.")
            else:
                print("⚠ No new documents loaded. Using existing database.")

    def reload_knowledge_base(self):
        """Reload knowledge base without overwriting existing entries"""
        self.load_knowledge_base(overwrite_existing=False)

    def answer_question(self, question: str, file_filter: Union[str, List[str]] = None, conversation_history: list = None) -> dict:
        """
        Answer a user question using the RAG pipeline
        Returns dict with answer, sources, and metadata

        Args:
            question: The user's question
            file_filter: Optional file filter for search
            conversation_history: List of previous Q&A pairs as dicts with 'question' and 'answer' keys
        """
        print(f"\n🔍 Processing: {question}")
        print("-" * 60)

        # Prepare conversation context
        conversation_context = ""
        if conversation_history and len(conversation_history) > 0:
            conversation_context = "\n\nCONVERSATION HISTORY:\n"
            for i, qa in enumerate(conversation_history[-3:], 1):  # Last 3 exchanges for context
                conversation_context += f"Q{i}: {qa['question']}\nA{i}: {qa['answer']}\n\n"
            conversation_context += "CURRENT QUESTION: " + question
            print(f"📝 Using conversation context from {len(conversation_history)} previous exchanges")

        try:
            # Retrieve relevant context
            search_results = self.searcher.search(
                question, top_k=config.TOP_K_RETRIEVAL, file_filter=file_filter)
            search_results = set(search_results)
            if search_results and len(search_results) > 0:
                # Build context from search results
                context_parts = []
            if search_results and len(search_results) > 0:
                # Build context from search results
                context_parts = []
                for chunk, file_name, similarity in search_results:
                    if similarity > 0.1:  # Only include results with meaningful similarity
                        context_parts.append(
                            f"[From {file_name} (confidence: {similarity:.2%})]")
                        context_parts.append(chunk)
                        context_parts.append("")

                context = "\n".join(context_parts)

                if context.strip():
                    print(
                        f"📚 Retrieved {len(search_results)} relevant chunks from documentation")
                else:
                    print("⚠ No highly relevant content found in knowledge base")
                    context = ""
            else:
                context = ""
                print("⚠ No relevant content found in knowledge base")

            # Generate answer
            print("💭 Generating answer with Gemma...")
            answer = self.answer_generator.generate_answer(question, context, conversation_context)

            # Return result
            result = {
                "question": question,
                "answer": answer,
                "sources_found": len(search_results),
                "search_results": [
                    {
                        "file": file_name,
                        "confidence": float(similarity),
                        "content_preview": chunk[:100] + "..." if len(chunk) > 100 else chunk
                    }
                    for chunk, file_name, similarity in search_results if similarity > 0.1
                ],
                "timestamp": datetime.now().isoformat()
            }

            return result

        except Exception as e:
            print(f"✗ Error in answer_question: {e}")
            import traceback
            traceback.print_exc()
            return {
                "question": question,
                "answer": f"Error processing question: {e}",
                "sources_found": 0,
                "search_results": [],
                "timestamp": datetime.now().isoformat()
            }

    def get_statistics(self) -> dict:
        """Get database statistics"""
        conn = sqlite3.connect(config.VECTOR_DB_FILE)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM documents")
        doc_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM chunks")
        chunk_count = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(chunk_count) FROM documents")
        total_chunks = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "documents_loaded": doc_count,
            "total_chunks": chunk_count,
            "vector_database": config.VECTOR_DB_FILE,
            "embedding_model": config.EMBEDDING_MODEL,
            "embedding_dimension": self.embedding_manager.embedding_dim
        }
