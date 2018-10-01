package com.yg.gqlwfdl.services

sealed class EntityOrId<TEntity : com.yg.gqlwfdl.services.Entity<TId>, TId> {
    val entityId: TId?
        get() {
            if (this is Entity<TEntity, TId>)
                this.entity.id?.let { return it }

            if (this is Id<TEntity, TId>)
                return this.id

            return null
        }

    class Id<TEntity : com.yg.gqlwfdl.services.Entity<TId>, TId>(val id: TId) : EntityOrId<TEntity, TId>()
    class Entity<TEntity : com.yg.gqlwfdl.services.Entity<TId>, TId>(val entity: TEntity) : EntityOrId<TEntity, TId>()
}

