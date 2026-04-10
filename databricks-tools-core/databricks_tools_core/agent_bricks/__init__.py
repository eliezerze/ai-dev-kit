"""
Agent Bricks - Manage Genie Spaces, Knowledge Assistants, and Supervisor Agents.

This module provides a unified interface for managing Agent Bricks resources:
- Knowledge Assistants (KA): Document-based Q&A systems
- Supervisor Agents (MAS): Multi-agent orchestration
- Genie Spaces: SQL-based data exploration
"""

from .manager import AgentBricksManager, TileExampleQueue, get_tile_example_queue
from .genie import manage_genie, ask_genie
from .agent_bricks_api import manage_ka, manage_mas
from .models import (
    # Enums
    EndpointStatus,
    Permission,
    TileType,
    # Data classes
    GenieIds,
    KAIds,
    MASIds,
    # TypedDicts
    BaseAgentDict,
    CuratedQuestionDict,
    EvaluationRunDict,
    GenieListInstructionsResponseDict,
    GenieListQuestionsResponseDict,
    GenieSpaceDict,
    InstructionDict,
    KnowledgeAssistantDict,
    KnowledgeAssistantExampleDict,
    KnowledgeAssistantListExamplesResponseDict,
    KnowledgeAssistantResponseDict,
    KnowledgeAssistantStatusDict,
    KnowledgeSourceDict,
    ListEvaluationRunsResponseDict,
    MultiAgentSupervisorDict,
    MultiAgentSupervisorExampleDict,
    MultiAgentSupervisorListExamplesResponseDict,
    MultiAgentSupervisorResponseDict,
    MultiAgentSupervisorStatusDict,
    TileDict,
)

__all__ = [
    # Main class
    "AgentBricksManager",
    # High-level API
    "manage_ka",
    "manage_mas",
    # Genie operations
    "manage_genie",
    "ask_genie",
    # Background queue
    "TileExampleQueue",
    "get_tile_example_queue",
    # Enums
    "EndpointStatus",
    "Permission",
    "TileType",
    # Data classes
    "GenieIds",
    "KAIds",
    "MASIds",
    # TypedDicts
    "BaseAgentDict",
    "CuratedQuestionDict",
    "EvaluationRunDict",
    "GenieListInstructionsResponseDict",
    "GenieListQuestionsResponseDict",
    "GenieSpaceDict",
    "InstructionDict",
    "KnowledgeAssistantDict",
    "KnowledgeAssistantExampleDict",
    "KnowledgeAssistantListExamplesResponseDict",
    "KnowledgeAssistantResponseDict",
    "KnowledgeAssistantStatusDict",
    "KnowledgeSourceDict",
    "ListEvaluationRunsResponseDict",
    "MultiAgentSupervisorDict",
    "MultiAgentSupervisorExampleDict",
    "MultiAgentSupervisorListExamplesResponseDict",
    "MultiAgentSupervisorResponseDict",
    "MultiAgentSupervisorStatusDict",
    "TileDict",
]
