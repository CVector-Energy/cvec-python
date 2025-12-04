import os
import uuid

from cvec import CVec
from cvec.models.agent_post import AgentPostRecommendation, AgentPostTag, Severity


def main() -> None:
    # Initialize CVec client
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://your-subdomain.cvector.dev"),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )

    recommendations = [
        AgentPostRecommendation(
            content="Critical recommendation",
            severity=Severity.CRITICAL,
        ),
        AgentPostRecommendation(
            content="Warning recommendation",
            severity=Severity.WARNING,
        ),
        AgentPostRecommendation(
            content="Info recommendation",
            severity=Severity.INFO,
        ),
    ]

    tags = [
        AgentPostTag(
            content="urgent",
            severity=Severity.CRITICAL,
        ),
        AgentPostTag(
            content="monitoring",
            severity=Severity.INFO,
        ),
    ]

    cvec.add_agent_post(
        title="Test post",
        author="Operational Agent",
        image_id=str(uuid.uuid4()),  # Replace with actual image UUID uploaded to S3
        content="SDK add post test.",
        recommendations=recommendations,
        tags=tags,
    )


if __name__ == "__main__":
    main()
