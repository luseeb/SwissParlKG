import os
from dotenv import load_dotenv

#Load environment variables
load_dotenv()

#Function for text cleaning
def clean_text(text, keep_punctuation=False, ensure_whitespace_after_punctuation=True):
    """Cleans text by removing html tags, non ascii chars, digits and optionally punctuation

    Parameters
    ----------
    text : str
        The text to clean
    keep_punctuation : bool
        Defines if punctuation should be kept
    ensure_whitespace_after_punctuation : bool
        Defines if a whitespace should be added after punctuation (.,;:!?) if one is missing
        
    Returns
    -------
    str
        The cleaned text
    """
    import re
    RE_TAGS = re.compile(r"<[^>]+>")
    RE_WSPACE = re.compile(r"\s+", re.IGNORECASE)
    
    # remove any html tags (< /br> often found)
    text = re.sub(RE_TAGS, " ", text)
    
    if ensure_whitespace_after_punctuation:
        RE_SPACE_AFTER_PUNKT = re.compile(r"(?<=[.,;:!?])(?=[A-Za-z])")
        text = re.sub(RE_SPACE_AFTER_PUNKT, " ", text)
    
    if keep_punctuation:
        RE_ASCII_PUNCTUATION = re.compile(r"[^A-Za-zÀ-ž,.!?ÄÖÜäöü0-9 ]", re.IGNORECASE)
        RE_SINGLECHAR_PUNCTUATION = re.compile(r"\b[A-Za-zÀ-ž,.!?ÄÖÜäöü0-9]\b", re.IGNORECASE)
    
        # keep only ASCII + European Chars and whitespace, no digits, keep punctuation
        text = re.sub(RE_ASCII_PUNCTUATION, " ", text)
        # convert all whitespaces (tabs etc.) to single wspace, keep punctuation
        text = re.sub(RE_SINGLECHAR_PUNCTUATION, " ", text)
    else:
        RE_ASCII = re.compile(r"[^A-Za-zÀ-ž ]", re.IGNORECASE)
        RE_SINGLECHAR = re.compile(r"\b[A-Za-zÀ-ž]\b", re.IGNORECASE)
        
        # keep only ASCII + European Chars and whitespace, no digits, no punctuation
        text = re.sub(RE_ASCII, " ", text)
        # convert all whitespaces (tabs etc.) to single wspace
        text = re.sub(RE_SINGLECHAR, " ", text)     
    
    text = re.sub(RE_WSPACE, " ", text)  
    return text

