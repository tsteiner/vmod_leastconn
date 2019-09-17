#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <cache/cache.h>
#include <cache/cache_director.h>
#include <cache/cache_backend.h>
#include <limits.h>

#include "vcc_leastconn_if.h"

struct vmod_leastconn_director {
  unsigned                   magic;
#define VMOD_LEASTCONN_DIRECTOR_MAGIC 0x56063f07
  pthread_rwlock_t           mutex;
  struct director            *director;
  unsigned                   backends_count;
  unsigned                   backends_size;
  VCL_BACKEND                *backends;
};

static unsigned v_matchproto_(vdi_healthy)
vmod_director_healthy(const struct director *director, const struct busyobj *busy_object, double *changed)
{
  unsigned retval = 0;
  unsigned backend_index;
  VCL_BACKEND backend;
  vtim_real c;
    
  struct vmod_leastconn_director *lcd;
  CHECK_OBJ_NOTNULL(director, DIRECTOR_MAGIC);
  CAST_OBJ_NOTNULL(lcd, director->priv, VMOD_LEASTCONN_DIRECTOR_MAGIC);
  CHECK_OBJ_ORNULL(busy_object, BUSYOBJ_MAGIC);
  AZ(pthread_rwlock_rdlock(&lcd->mutex));
  if (changed != NULL) {
    *changed = 0;
  }
  for (backend_index = 0; backend_index < lcd->backends_count; backend_index++) {
    backend = lcd->backends[backend_index];
    CHECK_OBJ_NOTNULL(backend, DIRECTOR_MAGIC);
    retval = backend->healthy(backend, busy_object, &c);
    if (changed != NULL && c > *changed) {
      *changed = c;
    }
    if (retval) {
      break;
    }
  }
  AZ(pthread_rwlock_unlock(&lcd->mutex));
  return retval;
}

static const struct director * v_matchproto_(vdi_resolve_f)
vmod_director_resolve(const struct director *director, struct worker *worker, struct busyobj *busy_object)
{
  unsigned backend_index;
  struct vmod_leastconn_director *lcd;
  struct backend *backend;
  unsigned selected_index = 0;
  unsigned least_connections = UINT_MAX;
  
  CHECK_OBJ_NOTNULL(director, DIRECTOR_MAGIC);
  CHECK_OBJ_NOTNULL(worker, WORKER_MAGIC);
  CHECK_OBJ_NOTNULL(busy_object, BUSYOBJ_MAGIC);
  CAST_OBJ_NOTNULL(lcd, director->priv, VMOD_LEASTCONN_DIRECTOR_MAGIC);
  
  for (backend_index = 0; backend_index < lcd->backends_count; backend_index++)
  {
    unsigned healthy;
    unsigned connections;
    
    CAST_OBJ_NOTNULL(backend, lcd->backends[backend_index]->priv, BACKEND_MAGIC);
    healthy = lcd->backends[backend_index]->healthy(lcd->backends[backend_index], busy_object, NULL);
    if (!healthy) {
      continue;
    }
    
    Lck_Lock(&backend->mtx);
    connections = backend->n_conn;
    Lck_Unlock(&backend->mtx);
    
    if (connections <= least_connections) {
      selected_index = backend_index;
      least_connections = connections;
    }
  }
  
  return lcd->backends[selected_index];
}

VCL_VOID vmod_director__init(VRT_CTX, struct vmod_leastconn_director **pointer, const char *name)
{
  struct vmod_leastconn_director *lcd;

  CHECK_OBJ_NOTNULL(ctx, VRT_CTX_MAGIC);
  AN(pointer);
  AZ(*pointer);
  ALLOC_OBJ(lcd, VMOD_LEASTCONN_DIRECTOR_MAGIC);
  AN(lcd);
  *pointer = lcd;
  ALLOC_OBJ(lcd->director, DIRECTOR_MAGIC);
  AN(lcd->director);
  lcd->director->name = name;
  REPLACE(lcd->director->vcl_name, "leastconn");
  lcd->director->priv = lcd;
  lcd->director->healthy = vmod_director_healthy;
  lcd->director->resolve = vmod_director_resolve;
  lcd->director->admin_health = VDI_AH_HEALTHY;
  
}

VCL_VOID vmod_director__fini(struct vmod_leastconn_director **pointer)
{
  struct vmod_leastconn_director *lcd;
  
  if (*pointer == NULL) {
    return;
  }
  
  TAKE_OBJ_NOTNULL(lcd, pointer, VMOD_LEASTCONN_DIRECTOR_MAGIC);
  free(lcd->backends);
  AZ(pthread_rwlock_destroy(&lcd->mutex));
  free(lcd->director->vcl_name);
  FREE_OBJ(lcd->director);
  FREE_OBJ(lcd);
}

VCL_VOID vmod_director_add_backend(VRT_CTX, struct vmod_leastconn_director *lcd, VCL_BACKEND backend)
{
  unsigned backend_index;
  struct backend *b;

  CHECK_OBJ_NOTNULL(ctx, VRT_CTX_MAGIC);
  CHECK_OBJ_NOTNULL(lcd, VMOD_LEASTCONN_DIRECTOR_MAGIC);
  
  if (backend == NULL) {
    VRT_fail(ctx, "%s: NULL backend cannot be added", lcd->director->vcl_name);
    return;
  }
  if (strncmp(backend->name, "backend", 8)) {
    VRT_fail(ctx, "%s: Directors are not supported as backends, only vanilla backends.", lcd->director->vcl_name);
    return;
  }
  
  AN(backend);
  AZ(pthread_rwlock_wrlock(&lcd->mutex));
  if (lcd->backends_count >= lcd->backends_size) {
    lcd->backends = realloc(lcd->backends, (lcd->backends_size + 16) * sizeof *lcd->backends);
    AN(lcd->backends);
    lcd->backends_size += 16;
  }
  assert(lcd->backends_count < lcd->backends_size);
  backend_index = lcd->backends_count++;
  lcd->backends[backend_index] = backend;
  AZ(pthread_rwlock_unlock(&lcd->mutex));
}

VCL_BACKEND vmod_director_backend(VRT_CTX, struct vmod_leastconn_director *lcd)
{
  CHECK_OBJ_NOTNULL(ctx, VRT_CTX_MAGIC);
  CHECK_OBJ_NOTNULL(lcd, VMOD_LEASTCONN_DIRECTOR_MAGIC);
  return lcd->director;
}
