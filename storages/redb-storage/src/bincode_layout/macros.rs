//! Generate independent layout tests from a single expected declaration.

macro_rules! enum_layout {
    (
        $test:ident: $target:ident {
            $(
                $variant:ident
                $( ( $( $arg:ident : $arg_type:ty ),* $(,)? ) )?
                $( { $( $field:ident : $field_type:ty ),* $(,)? } )?
            ),+ $(,)?
        }
    ) => {
        #[test]
        fn $test() {
            use $target as Target;
            fn field_types(value: Target) {
                match value {
                    $(
                        Target::$variant
                        $( ( $( $arg ),* ) )?
                        $( { $( $field ),* } )? => {
                            $( $( let _: $arg_type = $arg; )* )?
                            $( $( let _: $field_type = $field; )* )?
                        }
                    ),+
                }
            }
            let _: fn(Target) = field_types;
            let expected = [
                $(
                    (
                        stringify!($variant),
                        $crate::bincode_layout::macros::enum_layout!(
                            @shape
                            $( ( $( $arg : $arg_type ),* ) )?
                            $( { $( $field : $field_type ),* } )?
                        ),
                    )
                ),+
            ];
            assert_eq!(
                $crate::bincode_layout::serde_metadata::enum_layout::<Target>(),
                expected,
                "{} changed; review redb storage compatibility before updating this declaration",
                stringify!($target),
            );
        }
    };
    (@shape) => {
        $crate::bincode_layout::serde_metadata::Shape::Unit
    };
    (@shape ()) => {
        $crate::bincode_layout::serde_metadata::Shape::Tuple(0)
    };
    (@shape ($arg:ident : $arg_type:ty)) => {
        $crate::bincode_layout::serde_metadata::Shape::Newtype
    };
    (@shape ($( $arg:ident : $arg_type:ty ),+)) => {
        $crate::bincode_layout::serde_metadata::Shape::Tuple(
            [$( stringify!($arg) ),+].len()
        )
    };
    (@shape { $( $field:ident : $field_type:ty ),* }) => {
        $crate::bincode_layout::serde_metadata::Shape::Struct(
            &[$( stringify!($field) ),*]
        )
    };
}

macro_rules! struct_layout {
    ($test:ident: $target:ident { $( $field:ident : $field_type:ty ),* $(,)? }) => {
        #[test]
        fn $test() {
            use $target as Target;
            fn field_types(value: Target) {
                let Target { $( $field ),* } = value;
                $( let _: $field_type = $field; )*
            }
            let _: fn(Target) = field_types;
            let expected = $crate::bincode_layout::serde_metadata::Shape::Struct(
                &[$( stringify!($field) ),*]
            );
            assert_eq!(
                $crate::bincode_layout::serde_metadata::struct_layout::<Target>(),
                expected,
                "{} changed; review redb storage compatibility before updating this declaration",
                stringify!($target),
            );
        }
    };

    ($test:ident: $target:ident ( $( $field:ident : $field_type:ty ),* $(,)? )) => {
        #[test]
        fn $test() {
            use $target as Target;
            fn field_types(value: Target) {
                let Target($( $field ),*) = value;
                $( let _: $field_type = $field; )*
            }
            let _: fn(Target) = field_types;
            let expected = $crate::bincode_layout::macros::enum_layout!(
                @shape ($( $field : $field_type ),*)
            );
            assert_eq!(
                $crate::bincode_layout::serde_metadata::struct_layout::<Target>(),
                expected,
                "{} changed; review redb storage compatibility before updating this declaration",
                stringify!($target),
            );
        }
    };

}

pub(super) use {enum_layout, struct_layout};
