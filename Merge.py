import os
from pathlib import Path
import subprocess
import tempfile
import trimesh
from concurrent.futures import ProcessPoolExecutor, as_completed

# -------------------------------
# CONFIGURATION
# -------------------------------
INPUT_DIR = "G:\Sync\ForTomsSchool\Projects\AssetoCorsaMaps\DullesFull\modelLib"       # folder containing original .gltf/.glb
OUTPUT_DIR = "G:\Sync\ForTomsSchool\Projects\AssetoCorsaMaps\DullesFull\modelLib\merged_outputs"  # folder for merged GLBs
BATCH_SIZE = 300                # adjust for memory
MAX_WORKERS = 4                # number of parallel processes

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# -------------------------------
# FUNCTIONS
# -------------------------------

def convert_to_glb(input_file, temp_dir):
    """
    Convert GLTF to GLB with embedded textures using gltf-pipeline
    """
    output_file = Path(temp_dir) / f"{input_file.stem}.glb"
    try:
        subprocess.run(
            ["gltf-pipeline", "-i", str(input_file), "-o", str(output_file), "-b"],
            check=True,
            shell=True  # crucial for Windows
        )
        return output_file
    except Exception as e:
        print(f"[Conversion failed] {input_file}: {e}")
        return None


def load_scene(file_path):
    """
    Load GLB as a Trimesh scene
    """
    try:
        scene = trimesh.load(file_path, force='scene')
        return scene
    except Exception as e:
        print(f"[Load failed] {file_path}: {e}")
        return None


def merge_scenes(file_list):
    """
    Merge multiple scenes into one Trimesh Scene
    """
    combined = trimesh.Scene()
    for f in file_list:
        scene = load_scene(f)
        if scene is None:
            continue
        for geom_name, geom in scene.geometry.items():
            if hasattr(geom, 'visual') and geom.visual is not None:
                geom = geom.copy()
            combined.add_geometry(geom)
    return combined


def process_batch(batch_files, batch_index, temp_dir):
    """
    Convert batch to GLBs and merge
    """
    converted_files = []

    # Convert all files in batch to GLB
    for f in batch_files:
        glb_file = convert_to_glb(f, temp_dir)
        if glb_file:
            converted_files.append(glb_file)

    # Merge converted GLBs
    merged_scene = merge_scenes(converted_files)

    output_file = Path(OUTPUT_DIR) / f"merged_batch_{batch_index:04d}.glb"
    merged_scene.export(output_file)
    print(f"[Batch {batch_index}] Saved: {output_file}")

    return output_file


# -------------------------------
# MAIN
# -------------------------------

def main():
    files = list(Path(INPUT_DIR).glob("*.gltf")) + list(Path(INPUT_DIR).glob("*.glb"))
    print(f"Found {len(files)} files.")

    temp_dir = tempfile.TemporaryDirectory()

    # Split into batches
    batches = [files[i:i + BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]
    print(f"Processing {len(batches)} batches with up to {BATCH_SIZE} files each...")

    # Parallel processing
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_batch, batch, idx + 1, temp_dir.name): idx + 1
            for idx, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            batch_index = futures[future]
            try:
                result = future.result()
            except Exception as e:
                print(f"[Batch {batch_index} failed]: {e}")

    temp_dir.cleanup()
    print("All batches processed successfully!")


if __name__ == "__main__":
    main()