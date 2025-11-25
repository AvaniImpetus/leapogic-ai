"""
Streamlit UI for Gemma RAG System - mirrors the unrelated/app.py design
This file provides a Streamlit frontend that calls into the project's
`GemmaRAGSystem` backend to answer user questions from the knowledge base.
"""
import csv
import os
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as components

import config
from gemma_rag_system import GemmaRAGSystem

CSV_LOG_DIR = "logged_questions"
CSV_LOG_FILE = os.path.join(CSV_LOG_DIR, "feedback_log.csv")


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
            <div class="header-title">Leaplogic — Documentation Assistant</div>
            <p class="header-subtitle">Ask questions against your queries.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


class QuestionLogger:
    """Question logger using session state memory only."""

    def __init__(self):
        self._init_session_storage()
    
    def _init_session_storage(self):
        """Initialize session state storage for feedback logs."""
        if "feedback_logs" not in st.session_state:
            st.session_state.feedback_logs = []

    def log_feedback(self, question, answer, feedback, sources=None, framework=None, source=None, target=None):
        """Log user feedback to session state memory."""
        self._log_feedback_memory(question, answer, feedback, sources, framework, source, target)
        if feedback not in ["Not Marked"]:
            st.toast("✅ Feedback logged!", icon="📝")
    
    def _log_feedback_memory(self, question, answer, feedback, sources=None, framework=None, source=None, target=None):
        """Log feedback to session state memory."""
        sources_str = "; ".join(sources) if sources else ""
        timestamp = datetime.now().isoformat()
        
        log_entry = {
            "Question": question,
            "Answer": answer,
            "Feedback": feedback,
            "Sources": sources_str,
            "Framework": framework or "Not Specified",
            "Source": source if framework == "Leaplogic" else "",
            "Target": target if framework == "Leaplogic" else "",
            "Timestamp": timestamp
        }
        
        st.session_state.feedback_logs.append(log_entry)
    
    def get_feedback_logs(self):
        """Get all feedback logs from memory."""
        return st.session_state.get("feedback_logs", [])

    def get_feedback_stats(self):
        """Get statistics from session state feedback logs."""
        logs = self.get_feedback_logs()
        total = len(logs)
        positive = len([l for l in logs if l.get("Feedback", "").strip().lower() == "helpful"]) if total else 0
        negative = len([l for l in logs if l.get("Feedback", "").strip().lower() == "not helpful"]) if total else 0
        not_marked = len([l for l in logs if l.get("Feedback", "").strip() == "Not Marked"]) if total else 0
        return {"total": total, "positive": positive, "negative": negative, "not_marked": not_marked}
    
    def get_storage_info(self):
        """Get information about the current storage backend."""
        return {
            "type": "Session Memory",
            "description": "Session-based memory storage (no persistence)"
        }
    
    def update_feedback(self, question, answer, new_feedback):
        """Update feedback for an existing entry in session memory."""
        logs = st.session_state.get("feedback_logs", [])
        
        # Update matching entry (find most recent match)
        for i in range(len(logs) - 1, -1, -1):  # Start from end
            if logs[i]["Question"] == question and logs[i]["Answer"] == answer:
                logs[i]["Feedback"] = new_feedback
                # Also update framework info in case it changed
                logs[i]["Framework"] = st.session_state.get("kb_choice", "Not Specified")
                if st.session_state.get("kb_choice") == "Leaplogic":
                    logs[i]["Source"] = st.session_state.get("source", "Not Specified")
                    logs[i]["Target"] = st.session_state.get("target", "Not Specified")
                else:
                    logs[i]["Source"] = ""
                    logs[i]["Target"] = ""
                break
    
    def clear_all_logs(self):
        """Clear all feedback logs from session memory."""
        st.session_state.feedback_logs = []
    
    def export_csv(self):
        """Generate and export CSV data for download."""
        logs = self.get_feedback_logs()
        
        if not logs:
            return "Framework,Source,Target,Question,Answer,Feedback,Documentation Sources,Timestamp\n"  # Empty CSV with headers
        
        # Generate CSV content
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(["Framework", "Source", "Target", "Question", "Answer", "Feedback", "Documentation Sources", "Timestamp"])
        
        # Write data rows
        for log in logs:
            writer.writerow([
                log.get("Framework", ""),
                log.get("Source", ""),
                log.get("Target", ""),
                log.get("Question", ""),
                log.get("Answer", ""),
                log.get("Feedback", ""),
                log.get("Sources", ""),
                log.get("Timestamp", "")
            ])
        
        return output.getvalue()


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
        
        # Model configuration box
        st.markdown(f"""
            <div class="stat-card" style="margin-bottom: 10px;">
                <div class="stat-value">🤖</div>
                <div class="stat-label">Model</div>
                <div style="font-size: 0.8rem; color: #D57F00;">{config.GEMMA_MODEL}</div>
            </div>
        """, unsafe_allow_html=True)
        
        # Documentation configuration box
        if st.session_state.get("db_loaded", False):
            stats = system.get_statistics()
            doc_info = f"{stats.get('documents_loaded', 0)} files, {stats.get('total_chunks', 0)} chunks"
        else:
            doc_info = "Not loaded"
            
        st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value">📄</div>
                <div class="stat-label">Documentation</div>
                <div style="font-size: 0.8rem; color: #D57F00;">{doc_info}</div>
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
            <div style="color: black; font-weight: bold; font-size: 2.5rem;">👋 Welcome!</div>
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
            if st.button("🔤 How are Delete Queries converted?", use_container_width=True):
                process_user_question(
                    "How are Delete Queries converted in LeapLogic?")
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
                        # Update helpful feedback in CSV
                        if idx > 0:
                            user_msg = st.session_state.messages[idx - 1]
                            assistant_msg = message
                            logger = QuestionLogger()
                            logger.update_feedback(
                                question=user_msg["content"],
                                answer=assistant_msg["content"],
                                new_feedback="helpful"
                            )
                        st.session_state.messages[idx]["feedback_given"] = True
                        st.session_state.messages[idx]["feedback"] = "helpful"
                        st.rerun()
                with col2:
                    if st.button("👎", key=f"not_helpful_{idx}"):
                        # Update not helpful feedback in CSV
                        if idx > 0:
                            user_msg = st.session_state.messages[idx - 1]
                            assistant_msg = message
                            logger = QuestionLogger()
                            logger.update_feedback(
                                question=user_msg["content"],
                                answer=assistant_msg["content"],
                                new_feedback="not helpful"
                            )
                        st.session_state.messages[idx]["feedback_given"] = True
                        st.session_state.messages[idx]["feedback"] = "not helpful"
                        st.rerun()
            elif message["role"] == "assistant" and message.get("feedback_given", False):
                feedback = message.get("feedback", "")
                if feedback == "helpful":
                    st.caption("✓ Marked as helpful")
                elif feedback == "not helpful":
                    st.caption("⚠️ Marked for improvement")
                elif feedback == "Not Marked":
                    st.caption("○ Not marked")


def log_unmarked_feedback():
    """Log all previous assistant messages that haven't received feedback as 'Not Marked'."""
    logger = QuestionLogger()
    
    # Find all assistant messages without feedback
    for idx in range(len(st.session_state.messages)):
        message = st.session_state.messages[idx]
        
        # Check if it's an assistant message without feedback
        if message["role"] == "assistant" and not message.get("feedback_given", False):
            # Find the corresponding user question (should be the message before)
            if idx > 0:
                user_msg = st.session_state.messages[idx - 1]
                # Log as Not Marked
                logger.log_feedback(
                    question=user_msg["content"],
                    answer=message["content"],
                    feedback="Not Marked",
                    sources=message.get("source_docs", []),
                    framework=st.session_state.get("kb_choice", "Not Specified"),
                    source=st.session_state.get("source") if st.session_state.get("kb_choice") == "Leaplogic" else None,
                    target=st.session_state.get("target") if st.session_state.get("kb_choice") == "Leaplogic" else None
                )
                # Mark as feedback given so buttons disappear
                st.session_state.messages[idx]["feedback_given"] = True
                st.session_state.messages[idx]["feedback"] = "Not Marked"


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

    # Log any unmarked feedback before processing new question
    log_unmarked_feedback()

    st.session_state.messages.append(
        {"role": "user", "content": question})

    with st.chat_message("user", avatar="👤"):
        st.markdown(question)

    # Get assistant response
    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("🤔 Thinking..."):
            try:
                system = st.session_state.system

                # Extract conversation history (previous Q&A pairs)
                conversation_history = []
                for msg in st.session_state.messages:
                    if msg["role"] == "user":
                        # Find the corresponding assistant response
                        continue
                    elif msg["role"] == "assistant":
                        # Pair with the previous user message
                        user_msgs = [m for m in st.session_state.messages if m["role"] == "user"]
                        if user_msgs and len(user_msgs) > len(conversation_history):
                            prev_user_msg = user_msgs[len(conversation_history)]
                            conversation_history.append({
                                "question": prev_user_msg["content"],
                                "answer": msg["content"]
                            })

                result = system.answer_question(
                    question,
                    file_filter=st.session_state.get("file_filter"),
                    conversation_history=conversation_history
                )
                answer = result.get("answer", "")
                search_results = result.get("search_results", [])
                sources_md = format_sources(search_results)

                st.markdown(answer)
                if sources_md:
                    with st.expander("📚 View Sources", expanded=False):
                        st.markdown(sources_md)

                # Add message to session state
                message_idx = len(st.session_state.messages)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": answer,
                    "sources": sources_md,
                    "source_docs": [s.get("file") for s in search_results],
                    "feedback_given": False,
                })
                
                # Log question immediately with Not Marked status
                user_msg = st.session_state.messages[message_idx - 1]
                logger = QuestionLogger()
                logger.log_feedback(
                    question=user_msg["content"],
                    answer=answer,
                    feedback="Not Marked",
                    sources=[s.get("file") for s in search_results],
                    framework=st.session_state.get("kb_choice", "Not Specified"),
                    source=st.session_state.get("source") if st.session_state.get("kb_choice") == "Leaplogic" else None,
                    target=st.session_state.get("target") if st.session_state.get("kb_choice") == "Leaplogic" else None
                )

                # Display feedback buttons for the new response
                col1, col2, col3 = st.columns([1, 1, 8])
                with col1:
                    if st.button("👍", key=f"helpful_new_{message_idx}"):
                        # Update helpful feedback in CSV
                        user_msg = st.session_state.messages[message_idx - 1]
                        assistant_msg = st.session_state.messages[message_idx]
                        logger = QuestionLogger()
                        logger.update_feedback(
                            question=user_msg["content"],
                            answer=assistant_msg["content"],
                            new_feedback="helpful"
                        )
                        st.session_state.messages[message_idx]["feedback_given"] = True
                        st.session_state.messages[message_idx]["feedback"] = "helpful"
                        st.success("Thanks for your feedback!")
                        st.rerun()
                with col2:
                    if st.button("👎", key=f"not_helpful_new_{message_idx}"):
                        # Update not helpful feedback in CSV
                        user_msg = st.session_state.messages[message_idx - 1]
                        assistant_msg = st.session_state.messages[message_idx]
                        logger = QuestionLogger()
                        logger.update_feedback(
                            question=user_msg["content"],
                            answer=assistant_msg["content"],
                            new_feedback="not helpful"
                        )
                        st.session_state.messages[message_idx]["feedback_given"] = True
                        st.session_state.messages[message_idx]["feedback"] = "not helpful"
                        st.warning("Feedback logged. We'll improve this answer!")
                        st.rerun()

            except Exception as e:
                st.error(f"❌ Error: {e}")


def render_review_dashboard():
    st.markdown(
        """
        <div class="header-container">
            <div class="header-title">📋 Logged Questions & Feedback</div>
            <p class="header-subtitle">Review user feedback on questions and answers</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("⬅️ Back to Chat"):
        st.session_state.show_review_dashboard = False
        st.rerun()

    st.divider()
    logger = QuestionLogger()
    stats = logger.get_feedback_stats()
    logs = logger.get_feedback_logs()

    st.markdown("### 📊 Feedback Statistics")
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
                <div class="stat-value" style="color: #10b981;">{stats['positive']}</div>
                <div class="stat-label">Helpful</div>
            </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value" style="color: #ef4444;">{stats['negative']}</div>
                <div class="stat-label">Not Helpful</div>
            </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
            <div class="stat-card">
                <div class="stat-value" style="color: #9ca3af;">{stats['not_marked']}</div>
                <div class="stat-label">Not Marked</div>
            </div>
        """, unsafe_allow_html=True)

    st.divider()
    
    if not logs:
        st.info("No feedback logs found yet. Users need to mark responses as helpful or not helpful to see data here.")
        st.divider()
        return

    st.markdown("### 🔍 Filter Feedback")
    col1, col2 = st.columns(2)
    with col1:
        feedback_filter = st.selectbox("Filter by Feedback", ["All", "Helpful", "Not Helpful", "Not Marked"])
    with col2:
        framework_filter = st.selectbox("Filter by Knowledge Base", ["All", "Leaplogic", "Common Framework"])
    
    
    # CSV Download button
    csv_content = logger.export_csv()
    st.download_button(
        label="📥 Export to CSV",
        data=csv_content,
        file_name=f"feedback_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        help="Download for SharePoint sync"
    )

    # Filter logs based on selection
    filtered_logs = logs
    
    # Apply feedback filter
    if feedback_filter == "Helpful":
        filtered_logs = [log for log in filtered_logs if log.get("Feedback", "").strip().lower() == "helpful"]
    elif feedback_filter == "Not Helpful":
        filtered_logs = [log for log in filtered_logs if log.get("Feedback", "").strip().lower() == "not helpful"]
    elif feedback_filter == "Not Marked":
        filtered_logs = [log for log in filtered_logs if log.get("Feedback", "").strip() == "Not Marked"]
    
    # Apply framework filter
    if framework_filter == "Leaplogic":
        filtered_logs = [log for log in filtered_logs if log.get("Framework", "") == "Leaplogic"]
    elif framework_filter == "Common Framework":
        filtered_logs = [log for log in filtered_logs if log.get("Framework", "") == "Common Framework"]

    st.divider()
    
    if not filtered_logs:
        filter_desc = []
        if feedback_filter != "All":
            filter_desc.append(f"Feedback: {feedback_filter}")
        if framework_filter != "All":
            filter_desc.append(f"Knowledge Base: {framework_filter}")
        filter_text = " and ".join(filter_desc) if filter_desc else "current filters"
        st.info(f"No feedback entries found for {filter_text}")
    else:
        filter_desc = []
        if feedback_filter != "All":
            filter_desc.append(feedback_filter)
        if framework_filter != "All":
            filter_desc.append(framework_filter)
        filter_text = f" ({' + '.join(filter_desc)})" if filter_desc else ""
        st.markdown(f"### 📝 Feedback Entries{filter_text} ({len(filtered_logs)})")
        feedback_emoji = ""
        feedback_color = ""
        for idx, log in enumerate(reversed(filtered_logs)):
            feedback_value = log.get("Feedback", "").strip()
            if feedback_value.lower().strip() == "helpful":
                feedback_emoji = "👍"
                feedback_color = "#10b981"
            elif feedback_value.lower().strip() == "not helpful":
                feedback_emoji = "👎"
                feedback_color = "#ef4444"
            elif feedback_value.lower().strip() == "not marked":
                feedback_emoji = "○"
                feedback_color = "#9ca3af"
            
            with st.expander(f"{feedback_emoji} {log.get('Question', 'No question')[:80]}...", expanded=False):
                st.markdown(f"**❓ Question:** {log.get('Question', '')}")
                st.markdown(f"**🤖 Answer:** {log.get('Answer', '')}")
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.markdown(f"**📝 Feedback:** <span style='color: {feedback_color}; font-weight: bold;'>{log.get('Feedback', '')}</span>", unsafe_allow_html=True)
                    st.markdown(f"**🔧 Framework:** {log.get('Framework', '')}")
                with col2:
                    if log.get('Sources'):
                        st.markdown(f"**📚 Sources:** {log.get('Sources', '')}")
                    if log.get('Framework') == 'Leaplogic':
                        st.markdown(f"**📥 Source:** {log.get('Source', '')} | **Target:** {log.get('Target', '')}")


def main():
    st.set_page_config(page_title="Leaplogic AI Assistant", page_icon="🤖",
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

    # Check if we should show review dashboard first
    if st.session_state.get("show_review_dashboard", False):
        render_review_dashboard()
        return

    # Knowledge base selector in sidebar - only show when not in dashboard
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

    render_header()

    # Only render sidebar if system is initialized
    if st.session_state.get("system"):
        render_sidebar(st.session_state.system)

    if not st.session_state.get("db_loaded", False):
        st.markdown("""
            <div class="warning-box">
                <p style="font-weight: bold;">⚠️ Setup Required</p>
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
