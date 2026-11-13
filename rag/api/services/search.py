import json

from fastapi.responses import StreamingResponse
from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue
from sentence_transformers import SentenceTransformer

client = QdrantClient(host="qdrant", port=6333)
embed_model = SentenceTransformer("intfloat/multilingual-e5-base")
collection_name = "farcaster_casts"

async def search_vector(request):
    query_vector = embed_model.encode(request.query).tolist()

    filters = []

    if request.search_mode == "general" and request.fid:
        filters.append(FieldCondition(key="fid", match=MatchValue(value=request.fid)))


    filter_obj = Filter(must=filters) if filters else None

    results = client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=request.n_results,
        query_filter=filter_obj,
        with_payload=True,
    )

    async def event_generator():
        for hit in results:
            if hit.score >= request.min_score:
                yield json.dumps({"score": hit.score, "payload": hit.payload}) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")
