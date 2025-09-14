"""This module provides utilities to split text into smaller chunks"""


def chunk_text_with_overlap(text: str, chunk_size: int, overlap: float) -> list[str]:
    """Split a string into overlapping chunks of approximately equal size.

    The text is divided into segments of length `chunk_size`. Each chunk
    overlaps with the next by a fraction determined by `overlap`. This is
    useful in tasks such as natural language processing, where preserving
    context between adjacent segments is important (e.g., embeddings, search,
    summarization).

    Args:
        - text (str):
             the original text.
        - chunk_size (int): 
            maximum number of characters in each chunk.
        - overlap (float):
            Fraction of overlap between consecutive chunks, expressed as a 
            value between 0.0 (no overlap) and 1.0 (full overlap).
        
    Returns:
        - list[str]: 
            A list of text chunks, each chunk being at most `chunk_size` in
            length. Overlapping portions at the end of one chunk are repeated at
            the beginning of the next.

    Raises:
        ValueError: If `chunk_size` <= 0 or if `overlap` is not in the 
        range [0.0, 1.0].

    Example:
        >>> chunk_text_with_overlap("abcdefghij", chunk_size=4, overlap=0.25)
        ["abcd", "defg", "ghij"]
    """
    if chunk_size <= 0:
        raise ValueError('chunk size must be greater than zero')
    if overlap < 0.0 or overlap > 1.0:
        raise ValueError('overlap must be in the range [0.0, 1.0]')
    step = max(1, int(chunk_size * (1 - overlap)))
    return [text[i:i + chunk_size] for i in range(0, len(text), step)
            if i + chunk_size <= len(text)]


__all__ = ["chunk_text_with_overlap"]
