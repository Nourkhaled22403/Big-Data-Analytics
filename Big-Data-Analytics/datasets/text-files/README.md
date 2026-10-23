# Sample Dataset for MapReduce Testing

This directory contains multiple plain-text files that students can use to test their own Map-Reduce implementations (for example, Word Count, Inverted Index, and simple frequency analysis).

## Files

- `book_01.txt`
- `book_02.txt`
- `book_03.txt`
- `book_04.txt`
- `book_05.txt`

## Suggested Exercises

1. **Word Count**
   - Mapper emits `(word, 1)`
   - Reducer sums counts per word

2. **Top-K Words**
   - Use reducer output to find the most frequent words

3. **Inverted Index**
   - Mapper emits `(word, file_name)`
   - Reducer builds a unique list of files per word

## Notes

- Files are intentionally small so they can run quickly in local and educational setups.
- Content overlaps across files so reducers can be validated for key aggregation across multiple mapper outputs.
