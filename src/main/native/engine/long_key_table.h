#pragma once

#include "swiss_table.h"
#include <memory>
#include <vector>
#include <atomic>

namespace forl0 {

// One directory per state, shared by its key groups. A key-group tag prevents
// aliases even when a caller explicitly places the same key in two groups.
// Only a growing, sufficiently occupied small-integer domain uses pages;
// sparse, negative and large keys remain in the ordinary Swiss table.
template<class V> class LongKeyDirectory {
public:
    static constexpr size_t PAGE_BITS=8, PAGE_SIZE=1<<PAGE_BITS;
    static constexpr size_t LIMIT=1<<24, PAGE_COUNT=LIMIT/PAGE_SIZE;
    struct Page {
        std::atomic<int32_t> groups[PAGE_SIZE];
        V values[PAGE_SIZE];
        Page() { for(auto& g:groups)g.store(-1,std::memory_order_relaxed); }
    };
    explicit LongKeyDirectory(Allocator* alloc,int start,int groups):alloc_(alloc),counts_(groups),start_(start) {
        pages_=static_cast<std::atomic<Page*>*>(alloc_->allocate(PAGE_COUNT*sizeof(std::atomic<Page*>),8));
        for(size_t i=0;i<PAGE_COUNT;++i)new(pages_+i)std::atomic<Page*>(nullptr);
    }
    ~LongKeyDirectory() {
        for(auto i:allocated_) { auto* p=pages_[i].load(std::memory_order_relaxed);p->~Page(); alloc_->deallocate(p,sizeof(Page)); }
        if(pages_) alloc_->deallocate(pages_,PAGE_COUNT*sizeof(Page*));
    }
    Page* page(int64_t key) const {
        return uint64_t(key)<LIMIT && pages_ ? pages_[size_t(key)>>PAGE_BITS].load(std::memory_order_acquire) : nullptr;
    }
    Page* for_insert(int64_t key) {
        if(uint64_t(key)>=LIMIT) return nullptr;
        if(auto* p=page(key)) return p;
        if(size_t(key)>frontier_+4096 || allocated_.size()*PAGE_SIZE>peak_entries_*4+4096) return nullptr;
        void* mem=alloc_->allocate(sizeof(Page),std::max(size_t(8),alignof(Page)));
        Page* p;
        try { p=new(mem) Page(); }
        catch(...) { alloc_->deallocate(mem,sizeof(Page));throw; }
        const size_t index=size_t(key)>>PAGE_BITS;
        try { allocated_.push_back(index); }
        catch(...) { p->~Page();alloc_->deallocate(mem,sizeof(Page));throw; }
        pages_[index].store(p,std::memory_order_release);frontier_=std::max(frontier_,size_t(key));
        return p;
    }
    uintptr_t pages_address() const {return reinterpret_cast<uintptr_t>(pages_);}
    uintptr_t counts_address() {return reinterpret_cast<uintptr_t>(counts_.data());}
    uint64_t& count(int group){return counts_[group-start_];}
    void inserted(bool virgin) { if(virgin)++peak_entries_; }
    void erased() {}
    template<class F> void each(int group,F&& fn) const {
        for(size_t i=0;i<PAGE_COUNT;++i) {
            auto* p=pages_[i].load(std::memory_order_acquire);
            if(!p)continue;
            for(size_t j=0;j<PAGE_SIZE;++j)
                if(p->groups[j].load(std::memory_order_acquire)==group) fn(int64_t((i<<PAGE_BITS)+j),p->values[j]);
        }
    }
private:
    Allocator* alloc_;
    std::atomic<Page*>* pages_=nullptr;
    std::vector<size_t> allocated_;
    size_t peak_entries_=0,frontier_=0;
    std::vector<uint64_t> counts_;
    int start_;
};

template<class K,class V> class LongKeyTable:public SwissTable<K,V> {
public:
    using SwissTable<K,V>::SwissTable;
    void attach(LongKeyDirectory<V>*,int) {}
};

template<class V> class LongKeyTable<int64_t,V> {
    using Hash=SwissTable<int64_t,V>;
    Hash fallback_;
    LongKeyDirectory<V>* directory_=nullptr;
    int group_=0;
    template<class VV> std::pair<V*,bool> assign(int64_t key,VV&& value,bool overwrite) {
        if(directory_) {
            auto* p=directory_->page(key);
            size_t j=size_t(key)&255;
            if(p && p->groups[j].load(std::memory_order_acquire)==group_) {
                if(overwrite)p->values[j]=std::forward<VV>(value);
                return {&p->values[j],false};
            }
            // A key may have entered the fallback before its page was allocated.
            if(auto* old=fallback_.find(key)) {
                if(overwrite)*old=std::forward<VV>(value);
                return {old,false};
            }
            if(!p)p=directory_->for_insert(key);
            if(p && p->groups[j].load(std::memory_order_relaxed)<0) {
                const bool virgin=p->groups[j].load(std::memory_order_relaxed)==-1;
                p->values[j]=std::forward<VV>(value);
                p->groups[j].store(group_,std::memory_order_release);++directory_->count(group_);directory_->inserted(virgin);
                return {&p->values[j],true};
            }
        }
        return overwrite?fallback_.insert_or_assign(key,std::forward<VV>(value)):
                         fallback_.emplace(key,std::forward<VV>(value));
    }
public:
    explicit LongKeyTable(size_t capacity=16,Allocator* a=&DefaultAllocator::instance(),
                          size_t max_capacity=0,double load=.875):fallback_(capacity,a,max_capacity,load) {}
    void attach(LongKeyDirectory<V>* d,int group){directory_=d;group_=group;}
    V* find(int64_t key) {
        if(directory_)if(auto* p=directory_->page(key)) {
            auto j=size_t(key)&255;
            if(p->groups[j].load(std::memory_order_acquire)==group_)return &p->values[j];
        }
        return fallback_.find(key);
    }
    const V* find(int64_t key) const {return const_cast<LongKeyTable*>(this)->find(key);}
    std::pair<V*,bool> insert_or_assign(int64_t k,const V& v){return assign(k,v,true);}
    std::pair<V*,bool> insert_or_assign(int64_t k,V&& v){return assign(k,std::move(v),true);}
    template<class...Args>std::pair<V*,bool> emplace(int64_t k,Args&&...args) {
        if(auto* v=find(k))return {v,false};
        V v(std::forward<Args>(args)...);return assign(k,std::move(v),false);
    }
    bool erase(int64_t key) {
        if(directory_)if(auto* p=directory_->page(key)) {
            auto j=size_t(key)&255;
            if(p->groups[j].load(std::memory_order_acquire)==group_) {
                p->values[j]=V{};p->groups[j].store(-2-group_,std::memory_order_release);--directory_->count(group_);directory_->erased();return true;
            }
        }
        return fallback_.erase(key);
    }
    size_t size()const{return (directory_?directory_->count(group_):0)+fallback_.size();}
    bool empty()const{return size()==0;}
    size_t capacity()const{return fallback_.capacity();}
    void clear() {
        if(directory_)directory_->each(group_,[&](int64_t key,V&){erase(key);});
        fallback_.clear();
    }
    template<class F>void for_each(F&& f) {
        if(directory_)directory_->each(group_,f);
        fallback_.for_each(f);
    }
    template<class F>void for_each(F&& f)const {
        if(directory_)directory_->each(group_,[&](int64_t k,const V& v){f(k,v);});
        fallback_.for_each(f);
    }
};
} // namespace forl0
