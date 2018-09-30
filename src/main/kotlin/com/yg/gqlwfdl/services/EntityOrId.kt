package com.yg.gqlwfdl.services

sealed class EntityOrId<TId, TEntity : com.yg.gqlwfdl.services.Entity<TId>> {
    val entityId: TId?
        get() {
            if (this is Entity<TId, TEntity>)
                this.entity.id?.let { return it }

            if (this is Id<TId, TEntity>)
                return this.id

            return null
        }

    class Id<TId, TEntity : com.yg.gqlwfdl.services.Entity<TId>>(val id: TId) : EntityOrId<TId, TEntity>()
    class Entity<TId, TEntity : com.yg.gqlwfdl.services.Entity<TId>>(val entity: TEntity) : EntityOrId<TId, TEntity>()
}

