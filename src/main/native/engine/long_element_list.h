#pragma once
#include <vector>
#include <memory>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <stdexcept>

namespace forl0 {
// Explicit primitive-buffer header. Java may append/replace in existing capacity
// only when no snapshot is active and no immutable read view is published.
class LongElementList {
public:
    int64_t* first=nullptr;
    int64_t* last=nullptr;
    int64_t* limit=nullptr;
    uint64_t view_published=0;
    uint64_t version=0;
    mutable std::shared_ptr<void> read_cache;
    LongElementList()=default;
    LongElementList(const LongElementList& o){assign(o.begin(),o.end());}
    LongElementList(LongElementList&& o)noexcept{swap(o);}
    explicit LongElementList(const std::vector<int64_t>& o){assign(o.begin(),o.end());}
    LongElementList(std::initializer_list<int64_t> o){assign(o.begin(),o.end());}
    ~LongElementList(){delete[] first;}
    LongElementList& operator=(const LongElementList& o){if(this!=&o)assign(o.begin(),o.end());return *this;}
    LongElementList& operator=(const std::vector<int64_t>& o){assign(o.begin(),o.end());return *this;}
    LongElementList& operator=(LongElementList&& o)noexcept{
        const uint64_t next=version+1;LongElementList tmp(std::move(o));swap(tmp);version=next;return *this;
    }
    void swap(LongElementList& o)noexcept {
        std::swap(first,o.first);std::swap(last,o.last);std::swap(limit,o.limit);
        std::swap(view_published,o.view_published);std::swap(version,o.version);read_cache.swap(o.read_cache);
    }
    void invalidate_view(){read_cache.reset();view_published=0;++version;}
    void publish_view(std::shared_ptr<void> v)const {
        read_cache=std::move(v);const_cast<LongElementList*>(this)->view_published=1;
    }
    size_t size()const{return first?size_t(last-first):0;}
    size_t capacity()const{return first?size_t(limit-first):0;}
    bool empty()const{return last==first;}
    const int64_t* begin()const{return first;}
    const int64_t* end()const{return last;}
    const int64_t* data()const{return first;}
    int64_t* begin(){invalidate_view();return first;}
    int64_t* end(){invalidate_view();return last;}
    int64_t* data(){invalidate_view();return first;}
    const int64_t& operator[](size_t i)const{return first[i];}
    int64_t& operator[](size_t i){invalidate_view();return first[i];}
    void clear(){invalidate_view();last=first;}
    void reserve(size_t n) {
        if(n<=capacity())return;
        const size_t old=size();
        std::unique_ptr<int64_t[]> p(new int64_t[n]);
        if(old)std::memcpy(p.get(),first,old*sizeof(int64_t));
        delete[] first;first=p.release();last=first+old;limit=first+n;
    }
    void resize(size_t n) {
        invalidate_view();const size_t old=size();
        if(n>capacity())reserve(std::max(n,std::max(size_t(1),capacity()*2)));
        if(n>old)std::fill(first+old,first+n,0);
        last=first?first+n:nullptr;
    }
    void push_back(int64_t v) {
        invalidate_view();if(last==limit)reserve(std::max(size_t(1),capacity()*2));
        *last++=v;
    }
    template<class It>void assign(It begin,It end) {
        invalidate_view();const size_t n=begin==end?0:size_t(end-begin);reserve(n);
        if(n)std::copy(begin,end,first);
        last=first?first+n:nullptr;
    }
};
static_assert(offsetof(LongElementList,first)==0 && offsetof(LongElementList,last)==8
        && offsetof(LongElementList,limit)==16 && offsetof(LongElementList,view_published)==24
        && offsetof(LongElementList,version)==32,"PrimitiveLongAccess list header ABI");
}
