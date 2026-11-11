#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <initializer_list>
#include <iterator>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace forl0 {

// Compact nonnegative Long MapState dictionaries automatically use dense storage.
// Arbitrary keys promote them back to hashing; the public MapState API and
// checkpoint iteration still expose ordinary key/value pairs.
class LongInnerMap {
    struct DirectHeader {
        int64_t* values=nullptr;
        uint64_t* present=nullptr;
        size_t* used=nullptr;
        int64_t* keys=nullptr;
        uint64_t span=0;
    } direct_;
    using Hash = std::unordered_map<int64_t, int64_t>;
    struct Dense {
        std::vector<int64_t> values;
        std::vector<uint64_t> valid;
        size_t used = 0;
        explicit Dense(size_t n) : values(n), valid((n + 63) / 64) {}
        bool has(size_t i) const { return (valid[i / 64] >> (i % 64)) & 1; }
        void mark(size_t i) { valid[i / 64] |= uint64_t(1) << (i % 64); }
    };
    struct Small {
        int64_t keys[4],values[4];
        size_t used=0;
    };
    std::variant<Hash, Dense, Small> storage_{Small{}};
    Small* small(){return std::get_if<Small>(&storage_);}
    const Small* small()const{return std::get_if<Small>(&storage_);}
    Dense* dense() { return std::get_if<Dense>(&storage_); }
    const Dense* dense() const { return std::get_if<Dense>(&storage_); }
    Hash& hash() { return std::get<Hash>(storage_); }
    const Hash& hash() const { return std::get<Hash>(storage_); }
    void refresh_direct_header() {
        if(auto* m=small())direct_={m->values,nullptr,&m->used,m->keys,4};
        else if(auto* d=dense())direct_={d->values.data(),d->valid.data(),&d->used,nullptr,d->values.size()};
        else direct_={};
    }
    void promote_hash() {
        Hash h;
        if(auto* m=small()) {
            h.reserve(std::max(size_t(8),m->used));
            for(size_t i=0;i<m->used;++i)h.emplace(m->keys[i],m->values[i]);
        } else {
            const auto& d=*dense();h.reserve(d.used);
            for(size_t i=0;i<d.values.size();++i)if(d.has(i))h.emplace(int64_t(i),direct_.values[i]);
        }
        storage_=std::move(h);refresh_direct_header();
    }
public:
    template<bool Const> class Iter {
        using Owner = std::conditional_t<Const, const LongInnerMap, LongInnerMap>;
        using HIter = std::conditional_t<Const, Hash::const_iterator, Hash::iterator>;
        using Ref = std::conditional_t<Const, const int64_t&, int64_t&>;
    public:
        struct Entry { int64_t first; Ref second; };
    private:
        Owner* owner_ = nullptr;
        size_t index_ = 0;
        HIter hash_it_{};
        mutable std::optional<Entry> arrow_;
        void skip() {
            auto* d = owner_->dense();
            if(!d)return;
            while (index_ < d->values.size() && !d->has(index_)) ++index_;
        }
    public:
        Iter() = default;
        Iter(Owner* owner, size_t index) : owner_(owner), index_(index) { skip(); }
        Iter(Owner* owner, HIter it) : owner_(owner), hash_it_(it) {}
        Iter(const Iter& o) : owner_(o.owner_), index_(o.index_), hash_it_(o.hash_it_) {}
        Iter& operator=(const Iter& o) {
            owner_=o.owner_; index_=o.index_; hash_it_=o.hash_it_; arrow_.reset(); return *this;
        }
        Entry operator*() const {
            if (auto* d = owner_->dense()) return {static_cast<int64_t>(index_), owner_->direct_.values[index_]};
            if(auto* m=owner_->small())return {m->keys[index_],m->values[index_]};
            return {hash_it_->first, hash_it_->second};
        }
        const Entry* operator->() const { arrow_.emplace(operator*()); return &*arrow_; }
        Iter& operator++() {
            arrow_.reset();
            if (owner_->dense() || owner_->small()) { ++index_; skip(); } else ++hash_it_;
            return *this;
        }
        Iter operator++(int) { auto old=*this; ++*this; return old; }
        bool operator==(const Iter& o) const {
            if (owner_ != o.owner_) return false;
            if (!owner_) return true;
            return (owner_->dense() || owner_->small()) ? index_ == o.index_ : hash_it_ == o.hash_it_;
        }
        bool operator!=(const Iter& o) const { return !(*this == o); }
    };
    using iterator = Iter<false>;
    using const_iterator = Iter<true>;
    LongInnerMap(){refresh_direct_header();}
    LongInnerMap(const LongInnerMap& o):storage_(o.storage_){
        if(auto* d=dense();d&&!d->values.empty())std::memcpy(d->values.data(),o.direct_.values,d->values.size()*8);
        refresh_direct_header();
    }
    LongInnerMap(LongInnerMap&& o)noexcept:storage_(std::move(o.storage_)){
        auto* data=o.dense()?o.direct_.values:nullptr;refresh_direct_header();
        if(data&&dense())direct_.values=data;
        o.refresh_direct_header();
    }
    LongInnerMap& operator=(const LongInnerMap& o){
        if(this==&o)return *this;
        storage_=o.storage_;
        if(auto* d=dense();d&&!d->values.empty())std::memcpy(d->values.data(),o.direct_.values,d->values.size()*8);
        refresh_direct_header();return *this;
    }
    LongInnerMap& operator=(LongInnerMap&& o)noexcept{
        if(this==&o)return *this;
        auto* data=o.dense()?o.direct_.values:nullptr;
        storage_=std::move(o.storage_);refresh_direct_header();if(data&&dense())direct_.values=data;
        o.refresh_direct_header();return *this;
    }
    LongInnerMap(std::initializer_list<std::pair<int64_t,int64_t>> items) {
        for (const auto& kv : items) emplace(kv.first,kv.second);
        refresh_direct_header();
    }
    size_t size() const { auto* d=dense(); return d ? d->used : small()?small()->used:hash().size(); }
    bool empty() const { return size()==0; }
    bool is_dense() const { return dense()!=nullptr; }
    int64_t* full_dense_data(size_t& span) {
        auto* d=dense();
        if (!d || d->used!=d->values.size()) return nullptr;
        span=d->values.size();
        return direct_.values;
    }
    void attach_cache_data(int64_t* data) {
        auto* d=dense();if(!d||d->values.size()!=64||d->used!=64)throw std::logic_error("Only full64 dense rows may attach to L0");
        direct_.values=data;
    }
    bool detach_cache_data(int64_t* data) {
        auto* d=dense();if(!d||direct_.values!=data||direct_.values==d->values.data())return false;
        std::memcpy(d->values.data(),data,d->values.size()*8);direct_.values=d->values.data();return true;
    }
    void reserve(size_t n) { if(small() && n>4)promote_hash();if (!dense() && !small()) hash().reserve(n); }
    void clear() {
        if (auto* d=dense()) { std::fill(d->valid.begin(),d->valid.end(),0); d->used=0; }
        else if(auto* m=small())m->used=0;
        else hash().clear();
    }
    iterator begin() { return dense() || small()?iterator(this,size_t(0)):iterator(this,hash().begin()); }
    iterator end() { return dense() || small()?iterator(this,dense()?dense()->values.size():small()->used):iterator(this,hash().end()); }
    const_iterator begin() const { return dense() || small()?const_iterator(this,size_t(0)):const_iterator(this,hash().begin()); }
    const_iterator end() const { return dense() || small()?const_iterator(this,dense()?dense()->values.size():small()->used):const_iterator(this,hash().end()); }
    iterator find(int64_t key) {
        if (auto* d=dense()) {
            if (key>=0 && uint64_t(key)<d->values.size() && d->has(key)) return iterator(this,size_t(key));
            return end();
        }
        if(auto* m=small()) {
            for(size_t i=0;i<m->used;++i)if(m->keys[i]==key)return iterator(this,i);
            return end();
        }
        return iterator(this,hash().find(key));
    }
    const_iterator find(int64_t key) const {
        if (auto* d=dense()) {
            if (key>=0 && uint64_t(key)<d->values.size() && d->has(key)) return const_iterator(this,size_t(key));
            return end();
        }
        if(auto* m=small()) {
            for(size_t i=0;i<m->used;++i)if(m->keys[i]==key)return const_iterator(this,i);
            return end();
        }
        return const_iterator(this,hash().find(key));
    }
    const int64_t* find_value(int64_t key) const {
        if (const auto* d=dense()) {
            if (key>=0 && uint64_t(key)<d->values.size() && d->has(key)) return &direct_.values[key];
            return nullptr;
        }
        if(auto* m=small()) {
            for(size_t i=0;i<m->used;++i)if(m->keys[i]==key)return &m->values[i];
            return nullptr;
        }
        auto it=hash().find(key);
        return it==hash().end()?nullptr:&it->second;
    }
    void put_value(int64_t key,int64_t value) {
        if (auto* d=dense(); d && key>=0 && uint64_t(key)<d->values.size()) {
            if (!d->has(key)) {d->mark(key);++d->used;}
            direct_.values[key]=value;
            return;
        }
        (*this)[key]=value;
    }
    size_t count(int64_t key) const { return find_value(key)!=nullptr; }
    std::pair<iterator,bool> emplace(int64_t key,int64_t value) {
        if(auto* m=small()) {
            for(size_t i=0;i<m->used;++i)if(m->keys[i]==key)return {iterator(this,i),false};
            if(m->used<4) {
                size_t i=m->used++;m->keys[i]=key;m->values[i]=value;return {iterator(this,i),true};
            }
            promote_hash();
        }
        if (auto* d=dense()) {
            if (key>=0 && uint64_t(key)<d->values.size()) {
                bool added=!d->has(key);
                if (added) { direct_.values[key]=value; d->mark(key); ++d->used; }
                return {iterator(this,size_t(key)),added};
            }
            promote_hash();
        }
        auto result=hash().emplace(key,value);
        const bool added=result.second;
        const size_t n=hash().size();
        if (added && n>=32 && n<=256 && (n&(n-1))==0) {
            int64_t max_key=0;
            bool compact=true;
            for (const auto& kv:hash()) {
                if (kv.first<0 || kv.first>=256) { compact=false; break; }
                max_key=std::max(max_key,kv.first);
            }
            size_t span=64;
            while (span<=size_t(max_key)) span*=2;
            if (compact && span<=2*n && enable_dense(span)) return {find(key),added};
        }
        return {iterator(this,result.first),added};
    }
    int64_t& operator[](int64_t key) { return emplace(key,0).first->second; }
    size_t erase(int64_t key) {
        if(auto* m=small()) {
            for(size_t i=0;i<m->used;++i)if(m->keys[i]==key) {
                --m->used;m->keys[i]=m->keys[m->used];m->values[i]=m->values[m->used];return 1;
            }
            return 0;
        }
        if (auto* d=dense()) {
            if (key<0 || uint64_t(key)>=d->values.size() || !d->has(key)) return 0;
            d->valid[key/64] &= ~(uint64_t(1)<<(key%64)); --d->used; return 1;
        }
        return hash().erase(key);
    }
    static int64_t wrapping_add(int64_t a,int64_t b) {
        uint64_t bits=static_cast<uint64_t>(a)+static_cast<uint64_t>(b);
        int64_t result; std::memcpy(&result,&bits,sizeof(result)); return result;
    }
    int64_t add_sequence(int64_t start,int n,int64_t modulo,int64_t delta) {
        if(n<=0)return 0;
        if(modulo<=0 && start>std::numeric_limits<int64_t>::max()-(n-1))
            throw std::overflow_error("MapState range key overflow");
        int64_t key=modulo>0?start%modulo:start;
        if(modulo>0 && key<0)key+=modulo;
        if(n>=4)enable_dense(modulo);
        int64_t sum=0;
        for(int i=0;i<n;i++) {
            if(auto* d=dense(); d && key>=0 && uint64_t(key)<d->values.size()) {
                if(!d->has(key)) {direct_.values[key]=0;d->mark(key);++d->used;}
                direct_.values[key]=wrapping_add(direct_.values[key],delta);
                sum=wrapping_add(sum,direct_.values[key]);
            } else {
                auto& value=(*this)[key];value=wrapping_add(value,delta);
                sum=wrapping_add(sum,value);
            }
            if(i+1<n) {++key;if(modulo>0 && key==modulo)key=0;}
        }
        return sum;
    }
    int64_t sum_sequence(int64_t start,int n,int64_t modulo) const {
        if(n<=0)return 0;
        if(modulo<=0 && start>std::numeric_limits<int64_t>::max()-(n-1))
            throw std::overflow_error("MapState range key overflow");
        int64_t key=modulo>0?start%modulo:start;
        if(modulo>0 && key<0)key+=modulo;
        int64_t sum=0;
        for(int i=0;i<n;i++) {
            auto it=find(key);if(it!=end())sum=wrapping_add(sum,it->second);
            if(i+1<n) {++key;if(modulo>0 && key==modulo)key=0;}
        }
        return sum;
    }
    // Bounded range API only. Ordinary sparse / negative / large key maps do not
    // allocate a giant dense array. Conversion preserves every existing entry.
    bool enable_dense(int64_t span) {
        if (span<=0 || span>256) return false;
        if (auto* d=dense()) return d->values.size()>=size_t(span);
        for (const auto& kv:*this) if (kv.first<0 || kv.first>=span) return false;
        Dense d(static_cast<size_t>(span));
        for (const auto& kv:*this) { d.values[kv.first]=kv.second; d.mark(kv.first); ++d.used; }
        storage_=std::move(d); refresh_direct_header(); return true;
    }
};
} // namespace forl0
