# Crux Google Cloud Storage Checkpoints

# Usage
If you want to quickly try it out you should follow the [Official Crux installation](https://opencrux.com/reference/).

[comment]: <> (## Add a dependency)

[comment]: <> (Make sure to first add this module as a dependency:)

[comment]: <> ([![Clojars Project]&#40;https://img.shields.io/clojars/v/avisi-apps/crux-xodus.svg&#41;]&#40;https://clojars.org/avisi-apps/crux-xodus&#41;)


## Configure
And after that you can add a checkpointer which uses Google Cloud Storage

**EDN**
```clojure
{:crux/index-store
 {:kv-store
  {:crux/module 'crux.rocksdb/->kv-store
   :checkpointer {:crux/module 'crux.checkpoint/->checkpointer
                  :store {:crux/module 'avisi-apps.crux.google-cloud-storage.checkpoint/->cp-store
                          :storage-options {:crux/module 'avisi-apps.crux.google-cloud-storage.checkpoint/->storage-options
                                            :project "<your-project>"}
                          :bucket "<your-bucket>"}
                  :approx-frequency (Duration/ofHours 6)}}}
 }
```

For more information about configuring Checkpoints see: https://opencrux.com/reference/checkpointing.html

# Developer

## Releasing

First make sure the pom is up-to-date run
```
$ clojure -Spom
```

Edit the pom to have the wanted version and commit these changes.
Create a tag for the version for example:

```
$ git tag <your-version>
$ git publish origin <your-version>
```

Make sure you have everything setup to release to clojars by checking these [instructions](https://github.com/clojars/clojars-web/wiki/Pushing#maven).
After this is al done you can release by running:

```
$ mvn deploy
```

