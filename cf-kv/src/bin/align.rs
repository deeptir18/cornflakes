pub fn align_to_pow2(x: usize) -> usize {
    if x & (x - 1) == 0 {
        return x + (x == 0) as usize;
    }
    let mut size = x;
    size -= 1;
    size |= size >> 1;
    size |= size >> 2;
    size |= size >> 4;
    size |= size >> 8;
    size |= size >> 16;
    size |= size >> 32;
    size += 1;
    size += (size == 0) as usize;
    size
}

fn main() {
    for i in [0, 24, 64, 128, 8191, 7950, 3243, 15] {
        println!("x: {}, align up: {}", i, align_to_pow2(i));
    }
}
