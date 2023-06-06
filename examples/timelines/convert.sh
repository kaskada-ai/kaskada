#!/bin/bash

# Directory where your SVGs are located
# (same directory as this script)
svg_directory="$(dirname "$0")"

# Directory to save converted PNGs (same as input)
output_directory="$svg_directory"

for svg_file in "$svg_directory"/*.svg; do
  # Extract base name of file without extension
  base_name=$(basename "$svg_file" .svg)

  # Convert SVG to PNG with rsvg-convert
  rsvg-convert "$svg_file" -o "$output_directory/$base_name.png"

  # Trim PNG with ImageMagick
  #convert "$output_directory/$base_name.png" -trim "$output_directory/$base_name.png"

  echo "Converted ${svg_file} to ${output_directory}/${base_name}.png"
done