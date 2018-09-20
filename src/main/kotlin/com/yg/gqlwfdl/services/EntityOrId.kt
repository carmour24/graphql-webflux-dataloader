package com.yg.gqlwfdl.services

sealed class EntityOrId<TId, TEntity: com.yg.gqlwfdl.services.Entity<TId>> {
    class Id<TId, TEntity: com.yg.gqlwfdl.services.Entity<TId>>(val id: TId) : EntityOrId<TId, TEntity>()
    class Entity<TId, TEntity: com.yg.gqlwfdl.services.Entity<TId>>(val entity: TEntity) : EntityOrId<TId, TEntity>()
}

