/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_hub.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
namespace oceanbase
{
namespace storage
{
int ObFTDictHub::init()
{
  static constexpr int K_MAX_DICT_BUCKET = 128; // for now, only built-in dicts.
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_map_.create(K_MAX_DICT_BUCKET, "dict_map"))) {
  } else if (OB_FAIL(rw_dict_lock_.init(K_MAX_DICT_BUCKET))) {
  } else {
    is_inited_ = true;
  }
  return ret;
};

int ObFTDictHub::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  if (OB_NOT_NULL(shared_dict_main_)) {
    ObIFTDict *dict = ATOMIC_TAS(&shared_dict_main_, nullptr);
    dict->~ObIFTDict();
    shared_alloc_.free(dict);
  }
  if (OB_NOT_NULL(shared_dict_quan_)) {
    ObIFTDict *dict = ATOMIC_TAS(&shared_dict_quan_, nullptr);
    dict->~ObIFTDict();
    shared_alloc_.free(dict);
  }
  shared_main_container_.reset();
  shared_quan_container_.reset();
  shared_alloc_.reset();
  return ret;
}
int ObFTDictHub::build_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObBucketHashWLockGuard guard(rw_dict_lock_, key.hash());
    ret = build_cache_nolock(desc, container);
  }
  return ret;
}

int ObFTDictHub::build_cache_nolock(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTDictInfo info;
  container.reset();

  // try if valid with no recursive lock
  if (OB_FAIL(get_dict_info(key, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // dict not exist, make new one, by caller
      ret = OB_ENTRY_NOT_EXIST;
    } else {
    }
  } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
    } else {
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(ObFTRangeDict::build_cache_from_ik_dict(desc, container))) {
      } else if (FALSE_IT(info.range_count_ = container.get_handles().size())) {
      } else if (OB_FAIL(put_dict_info(key, info))) {
      }
    }
  }
  return ret;
}

int ObFTDictHub::load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObBucketHashRLockGuard guard(rw_dict_lock_, key.hash());
    ret = load_cache_nolock(desc, container);
  }
  return ret;
}

int ObFTDictHub::load_cache_nolock(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfo info;
  container.reset();
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  if (OB_FAIL(get_dict_info(key, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // dict not exist, make new one, by caller
      ret = OB_ENTRY_NOT_EXIST;
    } else {
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // dict not exist, make new one, by caller
    } else {
    }
  }

  return ret;
}


int ObFTDictHub::get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dict_map_.get_refactored(key, info))) {
    if (OB_HASH_NOT_EXIST != ret) {
    }
  }

  return ret;
}

int ObFTDictHub::put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  const int cover_exist_flag = 1;
  if (OB_FAIL(dict_map_.set_refactored(key, info, cover_exist_flag))) {
  }

  return ret;
}

int ObFTDictHub::get_shared_dict(const ObFTDictDesc &desc, ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  dict = nullptr;
  const bool is_main = (ObFTDictType::DICT_IK_MAIN == desc.type_);
  ObIFTDict *cached = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!is_main && ObFTDictType::DICT_IK_QUAN != desc.type_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    cached = is_main ? ATOMIC_LOAD(&shared_dict_main_) : ATOMIC_LOAD(&shared_dict_quan_);
    if (OB_NOT_NULL(cached)) {
      dict = cached;
    } else {
      // slow path: build once under the bucket lock, publish for later lock-free reads
      ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
      ObBucketHashWLockGuard guard(rw_dict_lock_, key.hash());
      cached = is_main ? ATOMIC_LOAD(&shared_dict_main_) : ATOMIC_LOAD(&shared_dict_quan_);
      if (OB_NOT_NULL(cached)) {
        dict = cached;
      } else {
        ObFTCacheRangeContainer &container = is_main ? shared_main_container_ : shared_quan_container_;
        if (OB_FAIL(load_cache_nolock(desc, container))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            if (OB_FAIL(build_cache_nolock(desc, container))) {
              container.reset();
            }
          }
        }
        if (OB_FAIL(ret)) {
          // already logged
        } else {
          void *buf = shared_alloc_.alloc(sizeof(ObFTRangeDict));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            container.reset();
          } else {
            ObFTRangeDict *range_dict = new (buf) ObFTRangeDict(shared_alloc_, &container, desc);
            if (OB_FAIL(range_dict->init())) {
              range_dict->~ObFTRangeDict();
              shared_alloc_.free(buf);
              container.reset();
            } else {
              if (is_main) {
                ATOMIC_STORE(&shared_dict_main_, range_dict);
              } else {
                ATOMIC_STORE(&shared_dict_quan_, range_dict);
              }
              dict = range_dict;
              LOG_INFO("build shared fts dict", K(static_cast<uint32_t>(desc.type_)), KP(range_dict));
            }
          }
        }
      }
    }
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
