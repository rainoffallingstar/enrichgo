library(hexSticker)
library(magick)

# Path to the generated icon
icon_path <- "inst/sticker/icon_yunwu.png"
processed_path <- "inst/sticker/icon_processed.png"

# Check if icon exists
if (!file.exists(icon_path)) {
  stop("Icon file not found: ", icon_path)
}

# Process the image: Remove white background
img <- image_read(icon_path)
# Fuzz allows for near-white pixels to be removed as well
img_trans <- image_transparent(img, "white", fuzz = 15)
image_write(img_trans, processed_path)

# Create the sticker
# Using a green/blue theme consistent with the package style
# p_color: Package name color
# h_fill: Hexagon fill color
# h_color: Hexagon border color
p = sticker(processed_path, 
        package="clusterProfiler", 
        p_size=18,           # Package name size
        p_y = 1.4,           # Package name position (higher)
        p_color = "#000000", # Black text for contrast on white
        s_x=1,               # Subplot x position
        s_y=0.75,            # Subplot y position (lower to fit under text)
        s_width=0.7,         # Subplot width
        h_fill="#FFFFFF",    # White background
        h_color="#336699",   # Blue border
        filename="inst/sticker/clusterProfiler_hex.png")
