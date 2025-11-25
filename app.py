"""
Streamlit UI for Gemma RAG System - mirrors the unrelated/app.py design
This file provides a Streamlit frontend that calls into the project's
`GemmaRAGSystem` backend to answer user questions from the knowledge base.
"""
import json
import os
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as components

import config
from gemma_rag_system import GemmaRAGSystem

LOG_FILE = "question_logs.json"


def apply_custom_css():
    """Load custom CSS from external file"""
    css_file = os.path.join(os.path.dirname(__file__), "static", "custom.css")

    if os.path.exists(css_file):
        with open(css_file, "r", encoding="utf-8") as f:
            css_content = f.read()
        st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)
    else:
        st.warning("Custom CSS file not found. Using default styling.")


def render_header():
    st.markdown(
        """
        <div class="header-container">
            <h1 class="header-title">Leaplogic — Documentation Assistant</h1>
            <p class="header-subtitle">Ask questions against your queries</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


class QuestionLogger:
    """Minimal question logger stored as JSONL (one JSON object per line)."""

    def __init__(self, file_path=LOG_FILE):
        self.file_path = file_path
        # Ensure directory exists
        directory = os.path.dirname(self.file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        # Create file if missing
        if not os.path.exists(self.file_path):
            open(self.file_path, "w", encoding="utf-8").close()

    def log(self, question, answer, sources=None, user_feedback=None, auto_detected=False):
        entry = {
            "timestamp": datetime.now().isoformat(),
            "question": question,
            "answer": answer,
            "sources": sources or [],
            "status": "pending",
            "notes": "",
            # "user_feedback": user_feedback or "",
            # "auto_detected": bool(auto_detected),
        }
        with open(self.file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def get_all_logs(self):
        print(self.file_path + "json file")
        logs = []
        if not os.path.exists(self.file_path):
            return logs
        with open(self.file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    logs.append(json.loads(line))
                except Exception:
                    # skip malformed lines
                    continue
        return logs

    def get_stats(self):
        logs = self.get_all_logs()
        total = len(logs)
        pending = len([l for l in logs if l.get(
            "status") == "pending"]) if total else 0
        user_reported = len([l for l in logs if l.get(
            "user_feedback") == "not_helpful"]) if total else 0
        resolved = len([l for l in logs if l.get(
            "status") == "resolved"]) if total else 0
        return {"total": total, "pending": pending, "user_reported": user_reported, "resolved": resolved}

    def update_status(self, timestamp, new_status):
        logs = self.get_all_logs()
        updated = False
        for entry in logs:
            if entry.get("timestamp") == timestamp:
                entry["status"] = new_status
                updated = True

        if updated:
            # overwrite file with updated logs
            with open(self.file_path, "w", encoding="utf-8") as f:
                for entry in logs:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def export_to_json(self, out_path="unanswered_questions.json"):
        logs = self.get_all_logs()
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)


def render_sidebar(system):
    with st.sidebar:
        st.markdown("### ℹ️ About")
        st.markdown("""
            <div class="info-box">
            <b>Leaplogic Documentation Assistant</b><br>
            Ask questions and get answers on leaplogic and common framework (wm-python).
            </div>
        """, unsafe_allow_html=True)

        st.divider()
        st.markdown("### ⚙️ Configuration")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"""
                <div class="stat-card">
                    <div class="stat-value">🤖</div>
                    <div class="stat-label">Model</div>
                    <div style="font-size: 0.8rem; color: #D57F00;">{config.GEMMA_MODEL}</div>
                </div>
            """, unsafe_allow_html=True)
        st.divider()

        if st.session_state.get("db_loaded", False):
            st.markdown("### 📊 Statistics")
            stats = system.get_statistics()
            num_messages = len(st.session_state.get("messages", []))
            st.markdown(f"""
                <div class="stat-card">
                    <div class="stat-value">{num_messages}</div>
                    <div class="stat-label">Messages in conversation</div>
                </div>
            """, unsafe_allow_html=True)

        st.divider()
        st.markdown("### 🔧 Actions")
        if st.button("🗑️ Clear Chat History", use_container_width=True, key="clear_chat_button"):
            st.session_state.messages = []
            st.success("Chat history cleared!")
            st.rerun()

        if st.button("🔄 Reload Database", use_container_width=True, key="clear_reload_button"):
            with st.spinner("Reloading vector database..."):
                try:
                    # Reload KB without overwriting existing
                    st.session_state.system.reload_knowledge_base()
                    st.success("Database reloaded successfully!")
                except Exception as e:
                    st.error(f"Failed to reload database: {e}")

        if st.button("📋 View Logged Questions", use_container_width=True, key="view_logged_questions_button"):
            st.session_state.show_review_dashboard = True
            st.rerun()

        st.divider()
        st.markdown("### 📡 Status")
        if st.session_state.get("db_loaded", False):
            st.markdown("""
                <div class="success-box">
                    <b>✅ System Ready</b><br>
                    Documentation loaded and ready to answer questions
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <div class="warning-box">
                    <b>⚠️ Setup Required</b><br>
                    Please run the ingestion script first
                </div>
            """, unsafe_allow_html=True)

        st.divider()
        with st.expander("💡 Quick Tips"):
            st.markdown("""
            - **Be specific**: Ask detailed questions for better answers
            - **Use examples**: Request code examples when needed
            - **Follow-up**: Ask clarifying questions based on previous answers
            - **Check sources**: View source documents to learn more
            """)


def initialize_session_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "system" not in st.session_state:
        with st.spinner("🔄 Initializing Gemma RAG System..."):
            try:
                st.session_state.system = GemmaRAGSystem()
                st.session_state.db_loaded = True
            except Exception as e:
                st.error(f"Failed to initialize system: {e}")
                st.session_state.system = None
                st.session_state.db_loaded = False
    if "show_review_dashboard" not in st.session_state:
        st.session_state.show_review_dashboard = False
    if "kb_choice" not in st.session_state:
        st.session_state.kb_choice = "Leaplogic"


def display_welcome_message():
    st.markdown(
        """
        <div style="text-align: center; padding: 3rem 0;">
            <h2 style="color: black;">👋 Welcome!</h2>
            <p style="font-size: 1.1rem; color: #6b7280; margin-top: 1rem;">I'm your AI assistant, ready to help you understand and use the docs.</p>
            <p style="color: #9ca3af; margin-top: 0.5rem;">Ask me anything about the docs below ⬇️</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Get current knowledge base selection
    kb_choice = st.session_state.get("kb_choice", "Leaplogic")
    file_filter = st.session_state.get("file_filter")
    is_leaplogic = file_filter is not None or kb_choice == "Leaplogic"

    kb_name = "Leaplogic" if is_leaplogic else "Common Framework"

    st.markdown(f"### 💭 Example Questions")
    col1, col2 = st.columns(2)

    if is_leaplogic:
        # Leaplogic questions
        with col1:
            if st.button("🔄 How does LeapLogic convert the QUALIFY clause?", use_container_width=True):
                process_user_question(
                    "How does LeapLogic convert the QUALIFY clause from Teradata to PySpark?")
                st.rerun()
        with col2:
            if st.button("🔤 What does the TO_CHAR function do?", use_container_width=True):
                process_user_question(
                    "What does the TO_CHAR function do in LeapLogic conversion?")
                st.rerun()

    else:
        # Common Framework questions
        with col1:
            if st.button("🏗️ What does the framework do?", use_container_width=True):
                process_user_question("What does the WMG framework do?")
                st.rerun()
        with col2:  
            if st.button("⚙️ How is a query executed on Glue?", use_container_width=True):
                process_user_question("How is a query executed on AWS Glue?")
                st.rerun()


def display_chat_history():
    for idx, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"], avatar="👤" if message["role"] == "user" else "🤖"):
            st.markdown(message["content"])
            if message.get("sources"):
                with st.expander("📚 View Sources", expanded=False):
                    st.markdown(message["sources"])

            if message["role"] == "assistant" and not message.get("feedback_given", False):
                col1, col2, col3 = st.columns([1, 1, 8])
                with col1:
                    if st.button("👍", key=f"helpful_{idx}"):
                        st.session_state.messages[idx]["feedback_given"] = True
                        st.session_state.messages[idx]["feedback"] = "helpful"
                        st.success("Thanks for your feedback!")
                        st.rerun()
                with col2:
                    if st.button("👎", key=f"not_helpful_{idx}"):
                        # Log the question as needing improvement
                        if idx > 0:
                            user_msg = st.session_state.messages[idx - 1]
                            assistant_msg = message
                            logger = QuestionLogger()
                            logger.log(
                                question=user_msg["content"],
                                answer=assistant_msg["content"],
                                sources=assistant_msg.get("source_docs", []),
                                user_feedback="not_helpful",
                                # auto_detected=False,
                            )
                        st.session_state.messages[idx]["feedback_given"] = True
                        st.session_state.messages[idx]["feedback"] = "not_helpful"
                        st.warning(
                            "Feedback logged. We'll improve this answer!")
                        st.rerun()
            elif message["role"] == "assistant" and message.get("feedback_given", False):
                feedback = message.get("feedback", "")
                if feedback == "helpful":
                    st.caption("✓ Marked as helpful")
                elif feedback == "not_helpful":
                    st.caption("⚠️ Marked for improvement")

            if "timestamp" in message:
                st.caption(f"⏰ {message['timestamp']}")


def format_sources(search_results):
    if not search_results:
        return ""
    lines = []
    for item in search_results:
        # each item: dict with file and confidence
        file = item.get("file")
        conf = item.get("confidence")
        lines.append(f"- **{file}** (confidence: {conf:.2%})\n\n")
    return "\n\n".join(lines)


def process_user_question(question: str):
    if not question:
        return

    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.messages.append(
        {"role": "user", "content": question, "timestamp": timestamp})

    with st.chat_message("user", avatar="👤"):
        st.markdown(question)
        st.caption(f"⏰ {timestamp}")

    # Get assistant response
    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("🤔 Thinking..."):
            try:
                system = st.session_state.system
                result = system.answer_question(
                    question, file_filter=st.session_state.get("file_filter"))
                answer = result.get("answer", "")
                search_results = result.get("search_results", [])
                sources_md = format_sources(search_results)

                st.markdown(answer)
                if sources_md:
                    with st.expander("📚 View Sources", expanded=False):
                        st.markdown(sources_md)

                timestamp = datetime.now().strftime("%H:%M:%S")
                st.caption(f"⏰ {timestamp}")

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": answer,
                    "sources": sources_md,
                    "source_docs": [s.get("file") for s in search_results],
                    "timestamp": timestamp,
                    "feedback_given": False,
                })

            except Exception as e:
                st.error(f"❌ Error: {e}")


def render_review_dashboard():
    st.markdown(
        """
        <div class="header-container">
            <h1 class="header-title">📋 Question Review Dashboard</h1>
            <p class="header-subtitle">Review and manage questions that need docs improvement</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("⬅️ Back to Chat"):
        st.session_state.show_review_dashboard = False
        st.rerun()

    st.divider()
    logger = QuestionLogger()
    stats = logger.get_stats()
    logs = logger.get_all_logs()

    st.markdown("### 📊 Statistics")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">{stats['total']}</div>
                <div class="stat-label">Total Questions</div>
            </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value" style="color: #f59e0b;">{stats['pending']}</div>
                <div class="stat-label">Pending Review</div>
            </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value" style="color: #ef4444;">{stats['user_reported']}</div>
                <div class="stat-label">User Reported</div>
            </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value" style="color: #10b981;">{stats['resolved']}</div>
                <div class="stat-label">Resolved</div>
            </div>
        """, unsafe_allow_html=True)

    st.divider()
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("📥 Export to JSON", use_container_width=True):
            logger.export_to_json()
            st.success("Exported to unanswered_questions.json")

    st.markdown("### 🔍 Filter Questions")
    filter_status = st.selectbox("Status", ["All", "Pending", "Resolved"])
    # filter_type = st.selectbox("Type", ["All", "Auto-detected", "User Reported"])

    filtered_logs = logs
    # if filter_status != "All":
    #     filtered_logs = [l for l in filtered_logs if l.get("status", "").lower() == filter_status.lower()]
    # if filter_type == "Auto-detected":
    #     filtered_logs = [l for l in filtered_logs if bool(l.get("auto_detected"))]
    # elif filter_type == "User Reported":
    #     filtered_logs = [l for l in filtered_logs if l.get("user_feedback") == "not_helpful"]

    st.divider()
    if not filtered_logs:
        st.info("No questions found matching the filters.")
    else:
        st.markdown(f"### 📝 Questions ({len(filtered_logs)})")
        for idx, log in enumerate(reversed(filtered_logs)):
            with st.expander(f"{'🔴' if log.get('user_feedback') == 'not_helpful' else '🟡'} {log['question'][:80]}...",
                             expanded=False):
                st.markdown(f"**❓ Question:** {log['question']}")
                st.markdown(f"**🤖 Answer:** {log['answer']}")
                if log.get('sources'):
                    st.markdown("**📚 Sources:**")
                    for source in log['sources']:
                        st.markdown(f"- {source}")
                else:
                    st.warning("No sources found")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.caption(f"⏰ {log['timestamp'][:19]}")
                with col2:
                    status_color = {"pending": "🟡", "resolved": "🟢"}
                    st.caption(
                        f"{status_color.get(log['status'], '⚪')} Status: {log['status']}")
                with col3:
                    if log.get('user_feedback') == 'not_helpful':
                        st.caption("👎 User reported")

                st.markdown("---")
                st.markdown("**Update Status:**")
                c1, c2, c3 = st.columns(3)
                # with c1:
                #     if st.button("Mark as Reviewed", key=f"review_{idx}"):
                #         logger.update_status(log['timestamp'], "reviewed")
                #         st.success("Marked as reviewed!")
                #         st.rerun()
                with c2:
                    if st.button("Mark as Resolved", key=f"resolve_{idx}"):
                        logger.update_status(log['timestamp'], "resolved")
                        st.success("Marked as resolved!")
                        st.rerun()
                with c3:
                    if st.button("Reset to Pending", key=f"pending_{idx}"):
                        logger.update_status(log['timestamp'], "pending")
                        st.success("Reset to pending!")
                        st.rerun()


def main():
    st.set_page_config(page_title="Gemma AI Assistant", page_icon="🤖",
                       layout="wide", initial_sidebar_state="expanded")
    apply_custom_css()
    st.logo(
        image="https://www.leaplogic.io/wp-content/themes/leaplogic/assets/images/logo-leaplogic-impetus.svg",
        size="medium", # Options: "small", "medium", "large"
        )
    initialize_session_state()

    # Initialize knowledge bases
    if 'system_leaplogic' not in st.session_state:
        with st.spinner("Loading Leaplogic knowledge base..."):
            st.session_state.system_leaplogic = GemmaRAGSystem(
                docs_folder="docs/leaplogic", db_file="vector_leaplogic.db")
    if 'system_common' not in st.session_state:
        with st.spinner("Loading Common Framework knowledge base..."):
            st.session_state.system_common = GemmaRAGSystem(
                docs_folder="docs/common", db_file="vector_common.db")

    st.session_state.db_loaded = True

    # Knowledge base selector in sidebar
    with st.sidebar:
        kb_choice = st.selectbox(
            "Select Knowledge Base",
            ["Leaplogic", "Common Framework"],
            index=0,  # Leaplogic as default
            help="Choose which documentation set to query: Leaplogic-specific docs or general framework docs"
        )
        st.session_state.kb_choice = kb_choice

        # Sub-options for Leaplogic
        if kb_choice == "Leaplogic":
            source = st.selectbox(
                "Source", ["Teradata"], index=0, key="source")
            target = st.selectbox(
                "Target", ["PySpark", "Redshift"], index=0, key="target")
            if target == "PySpark":
                file_filter = "teradata_to_pyspark.md"
            else:
                file_filter = "teradata_to_redshift.md"
        else:
            file_filter = None

    st.session_state.file_filter = file_filter

    if kb_choice == "Leaplogic":
        system = st.session_state.system_leaplogic
    else:
        system = st.session_state.system_common

    st.session_state.system = system  # For compatibility with existing code

    if st.session_state.get("show_review_dashboard", False):
        render_review_dashboard()
        return

    render_header()

    # Only render sidebar if system is initialized
    if st.session_state.get("system"):
        render_sidebar(st.session_state.system)

    if not st.session_state.get("db_loaded", False):
        st.markdown("""
            <div class="warning-box">
                <h3>⚠️ Setup Required</h3>
                <p>The vector database hasn't been created yet. Please run the ingestion script first.</p>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("### 📋 Setup Instructions")
        tab1, tab2, tab3 = st.tabs(
            ["1️⃣ Ingest Docs", "2️⃣ Start Chatting", "3️⃣ Helpers"])
        with tab1:
            st.markdown(
                "1. Add docs to the docs folder\n2. Run the ingestion script: `python ingest_docs.py`")
        with tab2:
            st.markdown("Refresh after ingestion and start asking questions.")
            if st.button("🔄 Refresh Page", use_container_width=True):
                st.rerun()
        with tab3:
            st.markdown("Helper commands and hints")
        return

    if not st.session_state.messages:
        display_welcome_message()

    user_input = st.chat_input("💬 Ask me anything about the docs...")

    if st.session_state.messages:
        display_chat_history()

    if user_input:
        process_user_question(user_input)


if __name__ == "__main__":
    main()
