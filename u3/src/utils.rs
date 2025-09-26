pub fn byte_offset_to_position(content: &str, offset: usize) -> Result<(usize, usize), &'static str> {
    let mut line = 1;
    let mut column = 1;
    let mut current_byte = 0;

    for c in content.chars() {
        if current_byte < offset {
            if c == '\n' {
                line += 1;
                column = 1;
            } else if c != '\r' {
                column += 1;
            }
        }

        current_byte += c.len_utf8();
    }

    Ok((line, column))
}

