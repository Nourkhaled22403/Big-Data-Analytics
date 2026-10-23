
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import re

NUM_REDUCERS = 2


# STEP 1: MAPPER
def mapper(file_path):
    """Read one split and emit (word, 1) pairs."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    words = re.findall(r"[a-z0-9]+(?:'[a-z0-9]+)?", text.lower())

    pairs = [(word, 1) for word in words]

    # STEP 2: COMBINER
    local_counts = defaultdict(int)
    for word, count in pairs:
        local_counts[word] += count

    print(
        f"[MAP] {file_path.name}: "
        f"{len(pairs)} tokens, "
        f"{len(local_counts)} unique words"
    )

    return file_path.name, dict(local_counts)


# STEP 3: PARTITIONER
def partition(word):
    """Assign each word to exactly one reducer."""
    return sum(ord(char) for char in word) % NUM_REDUCERS


# STEP 4: SHUFFLE AND SORT
def shuffle_and_sort(mapper_results):
    partitions = [defaultdict(int) for _ in range(NUM_REDUCERS)]

    for filename, local_counts in mapper_results:
        for word, count in local_counts.items():
            reducer_id = partition(word)
            partitions[reducer_id][word] += count

    sorted_partitions = [
        sorted(part.items()) for part in partitions
    ]

    for reducer_id, pairs in enumerate(sorted_partitions):
        print(
            f"[SHUFFLE] Reducer {reducer_id} receives "
            f"{len(pairs)} unique words"
        )

    return sorted_partitions


# STEP 5: REDUCER
def reducer(reducer_id, sorted_pairs, output_dir):
    output_file = output_dir / f"reducer{reducer_id}.txt"

    with output_file.open("w", encoding="utf-8") as f:
        for word, count in sorted_pairs:
            f.write(f"{word}\t{count}\n")

    print(f"[REDUCE {reducer_id}] Wrote {output_file}")


# MAIN PROGRAM
def run_mapreduce(input_dir, output_dir):
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    if not input_path.is_dir():
        raise FileNotFoundError(
            f"Input folder not found: {input_path}"
        )

    # Each .txt file is one split.
    input_files = sorted(
        p for p in input_path.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".txt"
        and p.name.lower() != "expected_word_count.txt"
    )

    if len(input_files) < 3:
        raise ValueError(
            "Please provide at least 3 input text files."
        )

    output_path.mkdir(parents=True, exist_ok=True)

    # Clear old reducer outputs before running again.
    for i in range(NUM_REDUCERS):
        old_file = output_path / f"reducer{i}.txt"
        if old_file.exists():
            old_file.unlink()

    print(f"\nInput splits: {len(input_files)}")
    print(f"Number of reducers: {NUM_REDUCERS}\n")

    # Run one mapper task per split concurrently.
    with ThreadPoolExecutor(
        max_workers=len(input_files)
    ) as executor:
        mapper_results = list(
            executor.map(mapper, input_files)
        )

    # Partition, shuffle, group, and sort.
    sorted_partitions = shuffle_and_sort(mapper_results)

    # Run both reducers concurrently.
    with ThreadPoolExecutor(
        max_workers=NUM_REDUCERS
    ) as executor:
        futures = [
            executor.submit(
                reducer, reducer_id, pairs, output_path
            )
            for reducer_id, pairs in enumerate(sorted_partitions)
        ]

        for future in futures:
            future.result()

    print("\nMapReduce completed successfully!")
    print(f"Output folder: {output_path.resolve()}")


if __name__ == "__main__":
    run_mapreduce(
        input_dir="datasets/text-files",
        output_dir="output"
    )
