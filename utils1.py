from PIL import Image

def load_image_from_local(image_path, image_resize=None):
    image = Image.open(image_path)

    if isinstance(image_resize, tuple):
        image = image.resize(image_resize)
    return image

