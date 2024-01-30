pub const MEDIA_FILES_EXTENSIONS: &'static [&str] = &["png", "jpeg", "gif", "mp3", "mov", "avi"];

pub fn is_media_file(url: &str) -> bool {
    let lowercased_url = url.to_lowercase();

    for extension in MEDIA_FILES_EXTENSIONS.iter() {
        if lowercased_url.ends_with(extension) {
            return true;
        }
    }

    return false;
}