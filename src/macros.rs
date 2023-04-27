#[macro_export]
macro_rules! stateless_mapper {
    ($name:ident, | $stage_param:ident : $stage_type:ty, $unit_param:ident : $unit_type:ty |  => $map:expr) => {
        #[derive(Default)]
        pub(crate) struct $name;

        #[async_trait::async_trait(?Send)]
        impl $crate::framework::Worker for $name {
            type Unit = $unit_type;
            type Stage = $stage_type;

            async fn bootstrap(_: &Self::Stage) -> Result<Self, $crate::framework::WorkerError> {
                Ok(Self)
            }

            async fn schedule(
                &mut self,
                stage: &mut Self::Stage,
            ) -> Result<$crate::framework::WorkSchedule<Self::Unit>, $crate::framework::WorkerError>
            {
                use $crate::framework::*;
                let msg = stage.input.recv().await.or_panic()?;

                Ok($crate::framework::WorkSchedule::Unit(msg.payload))
            }

            async fn execute(
                &mut self,
                __unit: &Self::Unit,
                __stage: &mut Self::Stage,
            ) -> Result<(), $crate::framework::WorkerError> {
                use $crate::framework::*;
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
macro_rules! stateless_flatmapper {
    ($name:ident, | $stage_param:ident : $stage_type:ty, $unit_param:ident : $unit_type:ty |  => $map:expr) => {
        #[derive(Default)]
        pub(crate) struct $name;

        #[async_trait::async_trait(?Send)]
        impl $crate::framework::Worker for $name {
            type Unit = $unit_type;
            type Stage = $stage_type;

            async fn bootstrap(_: &Self::Stage) -> Result<Self, $crate::framework::WorkerError> {
                Ok(Self)
            }

            async fn schedule(
                &mut self,
                stage: &mut Self::Stage,
            ) -> Result<$crate::framework::WorkSchedule<Self::Unit>, $crate::framework::WorkerError>
            {
                use $crate::framework::*;
                let msg = stage.input.recv().await.or_panic()?;

                Ok($crate::framework::WorkSchedule::Unit(msg.payload))
            }

            async fn execute(
                &mut self,
                __unit: &Self::Unit,
                __stage: &mut Self::Stage,
            ) -> Result<(), $crate::framework::WorkerError> {
                use $crate::framework::*;
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
