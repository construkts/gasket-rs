#[macro_export]
macro_rules! impl_mapper {
    (| $worker_param:ident : $worker_type:ty, $stage_param:ident : $stage_type:ty, $unit_param:ident : $unit_type:ty |  => $map:expr) => {
        #[async_trait::async_trait(?Send)]
        impl $crate::framework::Worker<$stage_type> for $worker_type {
            async fn bootstrap(
                stage: &$stage_type,
            ) -> Result<Self, $crate::framework::WorkerError> {
                Ok(Self::from(stage))
            }

            async fn schedule(
                &mut self,
                stage: &mut $stage_type,
            ) -> Result<$crate::framework::WorkSchedule<$unit_type>, $crate::framework::WorkerError>
            {
                use $crate::framework::*;
                let msg = stage.input.recv().await.or_panic()?;

                Ok($crate::framework::WorkSchedule::Unit(msg.payload))
            }

            async fn execute(
                &mut self,
                __unit: &$unit_type,
                __stage: &mut $stage_type,
            ) -> Result<(), $crate::framework::WorkerError> {
                use $crate::framework::*;
                let $worker_param = self;
                let $unit_param = __unit;
                let $stage_param = __stage;

                let out = { $map };

                $stage_param.output.send(out.into()).await.or_panic()?;

                Ok(())
            }
        }
    };
}

#[macro_export]
macro_rules! impl_splitter {
    (| $worker_param:ident : $worker_type:ty, $stage_param:ident : $stage_type:ty, $unit_param:ident : $unit_type:ty |  => $map:expr) => {
        #[async_trait::async_trait(?Send)]
        impl $crate::framework::Worker<$stage_type> for $worker_type {
            async fn bootstrap(
                stage: &$stage_type,
            ) -> Result<Self, $crate::framework::WorkerError> {
                Ok(Self::from(stage))
            }

            async fn schedule(
                &mut self,
                stage: &mut $stage_type,
            ) -> Result<$crate::framework::WorkSchedule<$unit_type>, $crate::framework::WorkerError>
            {
                use $crate::framework::*;
                let msg = stage.input.recv().await.or_panic()?;

                Ok($crate::framework::WorkSchedule::Unit(msg.payload))
            }

            async fn execute(
                &mut self,
                __unit: &$unit_type,
                __stage: &mut $stage_type,
            ) -> Result<(), $crate::framework::WorkerError> {
                use $crate::framework::*;
                let $worker_param = self;
                let $unit_param = __unit;
                let $stage_param = __stage;

                let out = { $map };

                for i in out.into_iter() {
                    $stage_param.output.send(i.into()).await.or_panic()?;
                }

                Ok(())
            }
        }
    };
}
