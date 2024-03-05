use solana_sdk::pubkey::Pubkey;

use crate::Result;

pub fn encode_u64x2_pubkey(seq: u64, slot: u64, pubkey: Pubkey) -> Vec<u8> {
    // create a key that is a concatenation of the seq, slot and the pubkey allocating memory immediately
    let slot_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    let mut key = Vec::with_capacity(slot_size * 2 + pubkey_size);
    key.extend_from_slice(&seq.to_be_bytes());
    key.extend_from_slice(&slot.to_be_bytes());
    key.extend_from_slice(&pubkey.to_bytes());
    key
}

pub fn decode_u64x2_pubkey(bytes: Vec<u8>) -> Result<(u64, u64, Pubkey)> {
    let slot_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    if bytes.len() != slot_size * 2 + pubkey_size {
        return Err(crate::StorageError::InvalidKeyLength);
    }
    let seq = u64::from_be_bytes(bytes[..slot_size].try_into()?);
    let slot = u64::from_be_bytes(bytes[slot_size..slot_size * 2].try_into()?);
    let pubkey = Pubkey::try_from(&bytes[slot_size * 2..])?;
    Ok((seq, slot, pubkey))
}

pub fn encode_u64_pubkey(slot: u64, pubkey: Pubkey) -> Vec<u8> {
    // create a key that is a concatenation of the slot and the pubkey allocating memory immediately
    let slot_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    let mut key = Vec::with_capacity(slot_size + pubkey_size);
    key.extend_from_slice(&slot.to_be_bytes());
    key.extend_from_slice(&pubkey.to_bytes());
    key
}

pub fn decode_u64_pubkey(bytes: Vec<u8>) -> Result<(u64, Pubkey)> {
    let slot_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    if bytes.len() != slot_size + pubkey_size {
        return Err(crate::StorageError::InvalidKeyLength);
    }
    let slot = u64::from_be_bytes(bytes[..slot_size].try_into()?);
    let pubkey = Pubkey::try_from(&bytes[slot_size..])?;
    Ok((slot, pubkey))
}

pub fn encode_pubkey_u64(pubkey: Pubkey, slot: u64) -> Vec<u8> {
    let pubkey_size = std::mem::size_of::<Pubkey>();
    let slot_size = std::mem::size_of::<u64>();
    let mut key = Vec::with_capacity(pubkey_size + slot_size);
    key.extend_from_slice(&pubkey.to_bytes());
    key.extend_from_slice(&slot.to_be_bytes());
    key
}

pub fn decode_pubkey_u64(bytes: Vec<u8>) -> Result<(Pubkey, u64)> {
    let pubkey_size = std::mem::size_of::<Pubkey>();
    let slot_size = std::mem::size_of::<u64>();
    if bytes.len() != slot_size + pubkey_size {
        return Err(crate::StorageError::InvalidKeyLength);
    }
    let pubkey = Pubkey::try_from(&bytes[..pubkey_size])?;
    let slot = u64::from_be_bytes(bytes[pubkey_size..].try_into()?);
    Ok((pubkey, slot))
}

pub fn encode_string(key: String) -> Vec<u8> {
    key.into_bytes()
}

pub fn decode_string(bytes: Vec<u8>) -> Result<String> {
    let key = String::from_utf8(bytes).unwrap_or_default();
    Ok(key)
}

pub fn encode_pubkey(pubkey: Pubkey) -> Vec<u8> {
    pubkey.to_bytes().to_vec()
}

pub fn decode_pubkey(bytes: Vec<u8>) -> Result<Pubkey> {
    let key = Pubkey::try_from(&bytes[..])?;
    Ok(key)
}

pub fn encode_u64(slot: u64) -> Vec<u8> {
    slot.to_be_bytes().to_vec()
}

pub fn decode_u64(bytes: Vec<u8>) -> Result<u64> {
    let slot = u64::from_be_bytes(bytes[..].try_into()?);
    Ok(slot)
}

pub fn decode_pubkey_u64_pubkey(bytes: Vec<u8>) -> Result<(Pubkey, u64, Pubkey)> {
    let u64_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    if bytes.len() != u64_size + pubkey_size * 2 {
        return Err(crate::StorageError::InvalidKeyLength);
    }
    let pk1 = Pubkey::try_from(&bytes[..pubkey_size])?;
    let slot = u64::from_be_bytes(bytes[pubkey_size..pubkey_size + u64_size].try_into()?);
    let pk2 = Pubkey::try_from(&bytes[pubkey_size + u64_size..])?;
    Ok((pk1, slot, pk2))
}

pub fn encode_pubkey_u64_pubkey(ask: (Pubkey, u64, Pubkey)) -> Vec<u8> {
    let u64_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    let mut key = Vec::with_capacity(u64_size + pubkey_size * 2);
    key.extend_from_slice(&ask.0.to_bytes());
    key.extend_from_slice(&ask.1.to_be_bytes());
    key.extend_from_slice(&ask.2.to_bytes());
    key
}

pub fn decode_pubkeyx2_u64_pubkey(bytes: Vec<u8>) -> Result<(Pubkey, Pubkey, u64, Pubkey)> {
    let u64_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    if bytes.len() != u64_size + pubkey_size * 3 {
        return Err(crate::StorageError::InvalidKeyLength);
    }
    let pk1 = Pubkey::try_from(&bytes[..pubkey_size])?;
    let pk2 = Pubkey::try_from(&bytes[pubkey_size..pubkey_size * 2])?;
    let slot = u64::from_be_bytes(bytes[pubkey_size * 2..pubkey_size * 2 + u64_size].try_into()?);
    let pk3 = Pubkey::try_from(&bytes[pubkey_size * 2 + u64_size..])?;
    Ok((pk1, pk2, slot, pk3))
}

pub fn encode_pubkeyx2_u64_pubkey(ask: (Pubkey, Pubkey, u64, Pubkey)) -> Vec<u8> {
    let u64_size = std::mem::size_of::<u64>();
    let pubkey_size = std::mem::size_of::<Pubkey>();
    let mut key = Vec::with_capacity(u64_size + pubkey_size * 3);
    key.extend_from_slice(&ask.0.to_bytes());
    key.extend_from_slice(&ask.1.to_bytes());
    key.extend_from_slice(&ask.2.to_be_bytes());
    key.extend_from_slice(&ask.3.to_bytes());
    key
}

#[cfg(test)]
mod tests {
    use solana_sdk::pubkey::Pubkey;

    // Import functions from the parent module
    use super::*;

    #[test]
    fn test_encode_decode_u64_pubkey() {
        let seq = 4321u64;
        let slot = 12345u64;
        let pubkey = Pubkey::new_unique(); // or some other way to create a Pubkey

        let encoded = encode_u64x2_pubkey(seq, slot, pubkey);
        let decoded = decode_u64x2_pubkey(encoded).unwrap();

        assert_eq!(decoded.0, seq);
        assert_eq!(decoded.1, slot);
        assert_eq!(decoded.2, pubkey);
    }

    #[test]
    fn test_encode_decode_pubkey() {
        let pubkey = Pubkey::new_unique(); // Create a test Pubkey

        let encoded = encode_pubkey(pubkey);
        let decoded = decode_pubkey(encoded).unwrap();

        assert_eq!(decoded, pubkey);
    }

    #[test]
    fn test_decode_u64_pubkey_invalid_data() {
        let invalid_data = vec![1, 2, 3]; // An intentionally invalid byte sequence
        assert!(decode_u64x2_pubkey(invalid_data).is_err());
    }

    #[test]
    fn test_decode_pubkey_invalid_data() {
        let invalid_data = vec![]; // An intentionally invalid byte sequence
        assert!(decode_pubkey(invalid_data).is_err());
    }

    #[test]
    fn test_encode_decode_pubkey_u64() {
        let slot = 12345u64;
        let pubkey = Pubkey::new_unique(); // or some other way to create a Pubkey

        let encoded = encode_pubkey_u64(pubkey, slot);
        let decoded = decode_pubkey_u64(encoded).unwrap();

        assert_eq!(decoded.0, pubkey);
        assert_eq!(decoded.1, slot);
    }

    #[test]
    fn test_pubkey_u64_pubkey() {
        let pk1 = Pubkey::new_unique(); // or some other way to create a Pubkey
        let slot = 12345u64;
        let pk2 = Pubkey::new_unique(); // or some other way to create a Pubkey

        let encoded = encode_pubkey_u64_pubkey((pk1, slot, pk2));
        let decoded = decode_pubkey_u64_pubkey(encoded).unwrap();

        assert_eq!(decoded.0, pk1);
        assert_eq!(decoded.1, slot);
        assert_eq!(decoded.2, pk2);
    }

    #[test]
    fn test_pubkeyx2_u64_pubkey() {
        let pk1 = Pubkey::new_unique(); // or some other way to create a Pubkey
        let pk2 = Pubkey::new_unique(); // or some other way to create a Pubkey
        let slot = 12345u64;
        let pk3 = Pubkey::new_unique(); // or some other way to create a Pubkey

        let encoded = encode_pubkeyx2_u64_pubkey((pk1, pk2, slot, pk3));
        let decoded = decode_pubkeyx2_u64_pubkey(encoded).unwrap();

        assert_eq!(decoded.0, pk1);
        assert_eq!(decoded.1, pk2);
        assert_eq!(decoded.2, slot);
        assert_eq!(decoded.3, pk3);
    }

    // Add more tests as needed...
}
