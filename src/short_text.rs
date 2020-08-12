use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ptr;
use std::{borrow::Borrow, hash::Hash, str};

#[derive(Copy)]
pub struct ShortText(u8, [u8; 256]);

impl ShortText {
    pub fn u8len(&self) -> u8 {
        self.0
    }

    pub fn len(&self) -> usize {
        self.u8len() as usize
    }

    pub fn build(text: &'static [u8]) -> ShortText {
        unsafe {
            let mut ret = ShortText(text.len() as u8, MaybeUninit::zeroed().assume_init());
            ret.1[..text.len()].copy_from_slice(text);
            ret
        }
    }
    
    pub unsafe fn new(len: u8) -> ShortText {
        ShortText(len, MaybeUninit::zeroed().assume_init())
    }

    pub unsafe fn get_buffer(&mut self) -> &mut [u8] {
        &mut self.1[0..self.0 as usize]
    }
}

impl Default for ShortText {
    fn default() -> Self {
        ShortText(0, unsafe { MaybeUninit::zeroed().assume_init() })
    }
}

impl Clone for ShortText {
    fn clone(&self) -> Self {
        let mut ret = ShortText(self.0, unsafe { MaybeUninit::zeroed().assume_init() });
        unsafe {
            ptr::copy_nonoverlapping(self.1.as_ptr(), ret.1.as_mut_ptr(), self.0 as usize);
        }
        ret
    }
    fn clone_from(&mut self, source: &Self) {
        self.0 = source.0;
        unsafe {
            ptr::copy_nonoverlapping(source.1.as_ptr(), self.1.as_mut_ptr(), source.0 as usize);
        }
    }
}

impl fmt::Display for ShortText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())?;
        Ok(())
    }
}

impl fmt::Debug for ShortText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"")?;
        f.write_str(self.as_ref())?;
        f.write_str("\"")?;
        Ok(())
    }
}

impl AsRef<str> for ShortText {
    fn as_ref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.1[0..self.0 as usize]) }
    }
}

impl Deref for ShortText {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_ref()
    }
}

impl PartialEq for ShortText {
    fn eq(&self, other: &Self) -> bool {
        if self.u8len() != other.u8len() {
            return false;
        }
        for i in 0..self.len() {
            if self.1[i] != other.1[i] {
                return false;
            }
        }
        return true;
    }
}

impl PartialEq<str> for ShortText {
    fn eq(&self, other: &str) -> bool {
        let obytes = other.as_bytes();
        if self.len() != obytes.len() {
            return false;
        }
        for i in 0..self.len() {
            if self.1[i] != obytes[i] {
                return false;
            }
        }
        return true;
    }
}

impl PartialEq<ShortText> for str {
    fn eq(&self, other: &ShortText) -> bool {
        other.eq(self)
    }
}

impl Eq for ShortText {}

impl Hash for ShortText {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl Borrow<str> for ShortText {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

impl PartialOrd for ShortText {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl PartialOrd<str> for ShortText {
    fn partial_cmp(&self, other: &str) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other)
    }
}

impl PartialOrd<ShortText> for str {
    fn partial_cmp(&self, other: &ShortText) -> Option<std::cmp::Ordering> {
        self.partial_cmp(other.as_ref())
    }
}

impl Ord for ShortText {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}
