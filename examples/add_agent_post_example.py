import os
import uuid

from cvec import CVec
from cvec.models.agent_post import AgentPostRecommendation, RecommendationType


def main() -> None:
    # Initialize CVec client
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://your-subdomain.cvector.dev"),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )

    recommendations = [
        AgentPostRecommendation(
            content="Critical recommendation",
            recommendation_type=RecommendationType.CRITICAL,
        ),
        AgentPostRecommendation(
            content="Warning recommendation",
            recommendation_type=RecommendationType.WARNING,
        ),
        AgentPostRecommendation(
            content="Info recommendation",
            recommendation_type=RecommendationType.INFO,
        ),
    ]

    cvec.add_agent_post(
        title="Test post",
        author="Operational Agent",
        image_id=str(uuid.uuid4()),  # Replace with actual image UUID uploaded to S3
        content="SDK add post test.",
        recommendations=recommendations,
    )


if __name__ == "__main__":
    main()
